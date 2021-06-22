(defpackage whirlog
  (:use cl)
  (:import-from sb-ext compare-and-swap)
  (:import-from sb-thread thread-yield)
  (:import-from util let-kw sym)
  (:export close-table column column-count column-value columns commit committed-record compare-column compare-key
	   compare-record context
	   decode-column decode-record delete-record do-context do-records do-sync
	   encode-column encode-key encode-record
	   file find-record
	   init-column init-record
	   key key?
	   let-tables
	   name new-column new-context new-table number-column
	   open-table
	   read-records record-count records rollback-changes
	   set-column-value store-record string-column
	   table table-records
	   with-db
	   tests))

(in-package whirlog)

(defparameter *delete* :D)
(defparameter *eof* (gensym))

(defvar *context*)
(defvar *path* #P"")

(defun new-context ()
  "Returns a fresh context"
  (rb:new-root :compare (lambda (x y)
			  (let* ((tbl (first x))
				 (res (rb:compare (name tbl) (name (first y)))))
			    (if (eq res :eq)
				(compare-key tbl (rest x) (rest y))
				res)))))

(defun get-change (tbl key)
  "Gets change for KEY in TBL"
  (rb:get-node *context* (cons tbl key)))

(defun (setf get-change) (rec tbl key)
  "Sets change to REC for KEY in TBL"
  (setf (rb:get-node *context* (cons tbl key)) rec))

(defun delete? (rec)
  "Returns T if REC is a delete"
  (eq rec *delete*))

(defun write-value (out x)
  "Writes X to OUT"
  (write x :stream out))

(defun rollback-changes ()
  "Rolls back changes in current context"
  (rb:clear *context*))

(defmacro do-sync ((tbl &optional slots) &body body)
  `(with-slots (busy? ,@slots) ,tbl
     (tagbody
      lock
	(unless (eq (compare-and-swap busy? nil t) nil)
	  (thread-yield)
	  (go lock)))
     
     (unwind-protect
	  (progn ,@body)
       (tagbody
	unlock
	  (unless (eq (compare-and-swap busy? t nil) t)
	    (thread-yield)
	    (go unlock))))))

(defun table-records (tbl key &key (sync? t))
  "Returns stack of records for KEY in TBL"
  (with-slots (records) tbl
    (if sync?
	(do-sync (tbl) (rb:get-node records key))
	(rb:get-node records key))))

(defun (setf table-records) (val tbl key &key (sync? t))
  "Sets stack of records for KEY in TBL to VAL"
  (with-slots (records) tbl
    (if sync?
	(do-sync (tbl) (setf (rb:get-node records key) val))
	(setf (rb:get-node records key) val))))

(defun find-table-record (tbl key &key (sync? t))
  "Returns record for KEY in TBL, or NIL if not found"
  (first (table-records tbl key :sync? sync?)))

(defun commit (&key (retries 3))
  "Commits changes in current context"
  (let (done)
    (labels ((undo ()
	       (dolist (c done)
		 (destructuring-bind (op tbl key rec) c
		   (do-sync (tbl (file records))
		     (ecase op
		       (:store
			(pop (table-records tbl key :sync? nil)))
		       (:delete 
			(push rec (table-records tbl key :sync? nil))))

		     (write-value file key)
		     (write-value file rec)
		     (terpri file)
		     (force-output file))))
	       (setf done nil)
	       nil)
	     (check ()
	       (rb:do-tree ((tbl . key) rec (rb:root-node *context*))
		 (let ((trec (find-table-record tbl key)))
		   (unless (or (eq rec *delete*) (eq (compare-record tbl trec rec) :eq))
		     (if (zerop retries)
			 (error "Commit failed: ~a ~a" trec rec) 
			 (undo))
		     (return-from check nil))))
	       t))
      (handler-case
	  (progn 
	    (rb:do-tree ((tbl . key) rec (rb:root-node *context*))
	      (do-sync (tbl (file records))
		(write-value file key)
		(write-value file rec)
		(terpri file)
		(force-output file)
		
		(if (delete? rec)
		    (push (list :delete tbl key (pop (table-records tbl key :sync? nil))) done)
		    (progn
		      (push rec (table-records tbl key :sync? nil))
		      (push (list :store tbl key *delete*) done)))))

	    (if (check)
		(rollback-changes)
		(progn
		  (thread-yield)
		  (commit :retries (decf retries)))))
	(t (e)
	  (undo)
	  (error e))))))

(defmacro do-context (() &body body)
  `(let ((*context* (new-context)))
     (handler-case
	 (progn
	   ,@body
	   (commit))
       (t (e)
	 (rollback-changes)
	 (error e)))))

(defclass column ()
  ((name :initarg :name
         :reader name)
   (key? :initarg :key?
	 :initform nil
         :reader key?)))

(defun new-column (name &rest opts)
  "Returns new columns for NAME and OPTS"
  (apply #'make-instance
         'column
         :name name
	 opts))

(defun column-value (rec col)
  "Returns value for COL in REC"
  (rest (assoc col rec)))

(defun (setf column-value) (val rec col)
  "Sets value for COL in REC to VAL"
  (setf (rest (assoc col rec)) val))

(defmethod init-column ((col column) rec)
  rec)

(defmethod encode-column ((col column) val)
  val)

(defmethod decode-column ((col column) val)
  val)

(defclass number-column (column)
  ())

(defmethod compare-column ((col number-column) x y)
  (cond
    ((< x y) :lt)
    ((> x y) :gt)
    (t :eq)))

(defclass string-column (column)
  ())

(defmethod compare-column ((col string-column) x y)  
  (cond
    ((string< x y) :lt)
    ((string> x y) :gt)
    (t :eq)))

(defclass table ()
  ((name :initarg :name
         :reader name)
   (busy? :initform nil
	  :reader busy?)
   (key :initarg :key
	:reader key)	 
   (columns :initarg :columns
            :reader columns)
   (file :initarg :file
         :reader file)
   (records :reader records)))

(defun new-table (name &rest cols)
  "Returns new table with NAME and COLS"
  (let ((k (remove-if-not #'key? cols)))
    (make-instance 'table
                   :name name
		   :key (make-array (length k) :element-type 'column :initial-contents k)
		   :columns cols)))

(defmethod compare-column ((col column) x y)
  (rb:compare x y))

(defun compare-record (tbl xs ys)
  (cond
    ((and (null xs) ys)
     (return-from compare-record :lt))
    ((and xs (null ys))
     (return-from compare-record :gt))
    ((null xs)
     (return-from compare-record :eq)))

  (dolist (c (columns tbl))
    (let ((x (column-value xs c))
	  (y (column-value ys c)))
      (cond
	((and (null x) y)
	 (return-from compare-record :lt))
	((and x (null y))
	 (return-from compare-record :gt))
	((null x)
	 (return-from compare-record :eq)))
      
      (ecase (compare-column c x y)
	(:lt (return-from compare-record :lt))
	(:gt (return-from compare-record :gt))
	(:eq))))
  
  :eq)

(defun init-record (tbl rec)
  (reduce (lambda (in col) (init-column col in)) (columns tbl) :initial-value rec))

(defun encode-record (tbl in)
  (let ((out (new-record)))
    (dolist (c (remove-if #'key? (columns tbl)))
      (let* ((cn (name c))
	     (v (column-value in cn)))
	(when v
	  (push (cons cn (encode-column c v)) out))))
    out))

(defun decode-record (tbl key in)
  (let ((out (new-record)))
    (let ((kcs (key tbl)))
      (dotimes (i (length kcs))
	(let ((c (aref kcs i)))
	  (push (cons (name c) (decode-column c (aref key i))) out))))
					
    (dolist (c (columns tbl))
      (let* ((cn (name c))
	     (v (column-value in cn)))
	(when v
	  (push (cons cn (decode-column c v)) out))))
    
    out))

(defun compare-key (tbl x y)
  (let ((k (key tbl)))
    (dotimes (i (length k))
      (ecase (compare-column (aref k i) (aref x i) (aref y i))
	(:lt (return-from compare-key :lt))
	(:gt (return-from compare-key :gt))
	(:eq))))
  :eq)

(defun encode-key (tbl in)
  (let* ((cols (key tbl))
	 (max (length cols))
	 (out (make-array max)))
    (dotimes (i max)
      (setf (aref out i) (encode-column (aref cols i) (aref in i))))
    out))

(defmethod initialize-instance :after ((tbl table) &rest args &key &allow-other-keys)
  (declare (ignore args))
  (setf (slot-value tbl 'records) (rb:new-root :compare (lambda (x y)
							  (compare-key tbl x y)))))

(defun read-value (in)
  "Reads value from IN"
  (read in nil *eof*))

(defun eof? (x)
  "Returns T if X is EOF"
  (eq x *eof*))

(defun read-records (tbl in &key (lazy? t) (sync? t))
  "Reads records from IN into TBL"
  (with-slots (records) tbl
    (let ((key (read-value in)))
      (unless (eof? key)
	
        (let ((pos (file-position in))
	      (rec (read-value in)))
          (when (eof? rec)
  	    (error "Missing record for key ~a" key))
	  (if (delete? rec)
	      (setf (table-records tbl key :sync? sync?) nil)
              (push (if lazy? pos rec) (table-records tbl key :sync? sync?)))
          (read-records tbl in))))))

(defun trim-path (in)
  (string-trim "*" in))

(defun open-table (tbl &key (lazy? t))
  "Opens and assigns TBL's file"
  (let ((path (merge-pathnames *path* (string-downcase (trim-path (symbol-name (name tbl)))))))
    (ensure-directories-exist path)
    (let ((file (open (format nil "~a.lisp" path)
		      :direction :io
		      :if-exists :overwrite
		      :if-does-not-exist :create)))
      (setf (slot-value tbl 'file) file)
      (read-records tbl file :lazy? lazy?))))

(defun close-table (tbl)
  "Closes and unbinds TBL's file"
  (close (file tbl))
  (slot-makunbound tbl 'file))

(defun column-count (tbl)
  "Returns number of columns in TBL"
  (length (columns tbl)))

(defun record-count (tbl)
  "Returns number of records in TBL"
  (rb:node-count (records tbl)))

(defmacro do-records ((rec tbl &key start) &body body)
  (let (($key (gensym)) ($recs (gensym)))
    `(rb:do-tree (,$key ,$recs (if ,start (rb:find-node (records ,tbl) ,start) (rb:root-node (records ,tbl))))
       (declare (ignore ,$key))
       (let ((,rec (first ,$recs)))
	 ,@body))))

(defun set-column-values (rec &rest flds)
  "Returns REC with updated FLDS"
  (labels ((acc (in out)
	     (if in
		 (acc (rest (rest in))
		      (cons (cons (first in) (second in))
			    out))
		 out)))
    (acc flds rec)))

(defun new-record (&rest flds)
  "Returns new record with FLDS"
  (apply #'set-column-values nil flds))

(defun record-key (tbl rec)
  "Returns key for REC in TBL"
  (let ((k (key tbl)))
    (make-array (length k)
		:initial-contents (map 'list
				       (lambda (c)
					 (column-value rec (name c)))
				       k))))

(defmacro with-db ((path (&rest tbls) &key (lazy? t)) &body body)
  (let (($tbl (gensym))
	($tbls (gensym)))
    `(let ((,$tbls (list ,@tbls))
	   (*path* (merge-pathnames *path* ,(or path #P""))))
       (dolist (,$tbl ,$tbls)
	 (open-table ,$tbl :lazy? ,lazy?))
       (unwind-protect
	    (progn ,@body)
	 (dolist (,$tbl ,$tbls)
	   (close-table ,$tbl))))))

(defmacro let-tables ((&rest tables) &body body)
  (labels ((bind (name &rest cols)
	     `(,name (new-table ',name
				,@(mapcar (lambda (c)
					    (if (listp c)
						(let-kw (c (ct :type))
						  `(make-instance ',(if ct (sym ct '-column) 'column)
								  :name ',(first c)
								  ,@(rest c)))
						`(make-instance 'column :name ',c)))
					  cols)))))
    `(let (,@(mapcar (lambda (x) (apply #'bind x)) tables)) 
       ,@body)))

(defun store-record (tbl rec)
  "Stores REC in TBL"
  (setf (get-change tbl (encode-key tbl (record-key tbl rec))) (encode-record tbl rec)))

(defmacro if-changed ((tbl key var) x y)
  (let ((rec (gensym)))
    `(let ((,rec (get-change ,tbl ,key)))
       (if ,rec
	   (let ((,var ,rec)) ,x)
	   ,y))))

(defun committed-record (tbl key &key (version 0) (sync? t))
  (flet ((get-rec ()
	   (let ((pos-or-rec (nth version (table-records tbl key :sync? nil))))
	     (typecase pos-or-rec
	       (integer
		(let ((in (file tbl)))
		  (file-position in pos-or-rec)

		  (let ((rec (read-value in)))
		    (setf (committed-record tbl key :version version :sync? nil) rec)
		    rec)))
	       (t pos-or-rec)))))
    (if sync?
	(do-sync (tbl)
	  (get-rec))
	(get-rec))))

(defun (setf committed-record) (rec tbl key &key (version 0) (sync? t))
  (setf (nth version (table-records tbl key :sync? sync?)) rec))

(defun find-record-encoded (tbl key &key (version 0) (sync? t))
  "Returns record for KEY in TBL if found, otherwise NIL"
  (let ((rec (if-changed (tbl key rec)
			 rec
			 (decode-record tbl key (committed-record tbl key :version version :sync? sync?)))))
    (unless (delete? rec) rec)))

(defun find-record (tbl key &key (version 0) (sync? t))
  (find-record-encoded tbl (encode-key tbl key) :version version :sync? sync?))

(defun delete-record (tbl key &key (sync? t))
  "Deletes REC from TBL"
  (let ((k (encode-key tbl key)))
    (unless (find-record-encoded tbl k :sync? sync?)
      (error "Record not found: ~a ~a" (name tbl) key))
    (setf (get-change tbl k) *delete*)))

(defun delete-if-exists (path)
  (when (probe-file path)
    (delete-file path)))

(defun test-setup ()
  (delete-if-exists "/tmp/whirlog/tbl.lisp"))

(defmacro with-test-db ((&rest tables) &body body)
  `(with-db ("/tmp/whirlog/" (,@tables)) ,@body))

(defun table-tests ()
  (test-setup)
  
  (let-tables ((tbl (key :key? t) val))
    (assert (string= (name tbl) 'tbl))
    (assert (= (column-count tbl) 2))
    (assert (eq (name (aref (key tbl) 0)) 'key))
    (with-test-db (tbl)
      (assert (= (record-count tbl) 0)))))

(defun record-tests ()
  (test-setup)
  
  (let-tables ((tbl (key :key? t) val))
    (with-test-db (tbl)
      (let ((rec (new-record 'key "foo" 'val "bar")))
	(assert (equalp #("foo") (record-key tbl rec)))
	
	
	(do-context ()
	  (store-record tbl rec)
          (assert (string= (column-value (find-record tbl #("foo")) 'val)
                           "bar")))

	(do-context ()
          (let ((rec (set-column-values rec 'val "baz")))
            (store-record tbl rec))

          (assert (string= (column-value (find-record tbl #("foo")) 'val) "baz"))))
      
      (do-context ()
	(delete-record tbl #("foo"))
	(assert (null (find-record tbl #("foo"))))))))

(defun reload-tests ()
  (test-setup)
  
  (let-tables ((tbl (key :key? t) val))
    (with-test-db (tbl)
      (let ((rec (new-record 'key "foo" 'val "bar")))
	(do-context ()
	  (store-record tbl rec))))

    (with-test-db (tbl)
      (do-context ()
	(assert (string= (column-value (find-record tbl #("foo")) 'val) "bar"))))))

(defun committed-tests ()
  (test-setup)
  
  (let-tables ((tbl (key :key? t) (val :type number)))
    (with-test-db (tbl)
      (let ((rec (new-record 'key :foo 'val 1)))
	(do-context ()
	  (store-record tbl rec))
	(let ((rec (set-column-values rec 'val 2)))
	  (do-context ()
	    (store-record tbl rec))))

      (assert (eq (column-value (committed-record tbl #(:foo) :version 0) 'val) 2))
      (assert (eq (column-value (committed-record tbl #(:foo) :version 1) 'val) 1))

      (setf (column-value (committed-record tbl #(:foo) :version 1) 'val) 3)
      (assert (eq (column-value (committed-record tbl #(:foo) :version 1) 'val) 3))

      (setf (committed-record tbl #(:foo) :version 0) (new-record 'key :foo 'val 4))
      (assert (eq (column-value (committed-record tbl #(:foo) :version 0) 'val) 4)))))

(defun tests ()
  (table-tests)
  (record-tests)
  (reload-tests)
  (committed-tests))
