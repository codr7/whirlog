(defpackage whirlog
  (:use cl)
  (:export
   close-table column column-count columns
   delete-record
   file find-record
   let-tables
   name new-column new-table
   open-table
   primary-key primary-key?
   read-records record-count records
   store-record
   table
   with-open-tables
   tests))

(in-package whirlog)

(defparameter *delete* 'd)
(defparameter *eof* (gensym))

(defclass table ()
  ((name :initarg :name
         :reader name)
   (primary-key :initarg :primary-key
		:reader primary-key)	 
   (columns :initarg :columns
            :reader columns)
   (file :initarg :file
         :reader file)
   (records :initform (make-hash-table :test 'equal)
            :reader records)))

(defun new-table (nam &rest cols)
  "Returns new table with NAM and COLS"
  (make-instance 'table
                 :name nam
		 :primary-key (remove-if-not #'primary-key? cols)
                 :columns cols))

(defun read-value (in)
  (read in nil *eof*))

(defun eof? (x)
  (eq x *eof*))

(defun delete? (x)
  (eq x *delete*))

(defun read-records (tbl in)
  "Reads records from IN into TBL"
  (with-slots (records) tbl
    (let ((key (read-value in)))
      (unless (eof? key)
        (let ((rec (read-value in)))
          (when (eof? rec)
  	    (error "Missing record for key ~a" key))
	  (if (delete? rec)
	    (remhash key records)
            (setf (gethash key records) rec))
          (read-records tbl in))))))

(defun open-table (tbl)
  "Opens and assigns TBL's file"
  (let ((fil (open (format nil "~a.tbl" (string-downcase (symbol-name (name tbl))))
                   :direction :io
		   :if-exists :overwrite
		   :if-does-not-exist :create)))
    (setf (slot-value tbl 'file) fil)
    (read-records tbl fil)))

(defun close-table (tbl)
  "Closes and unbinds TBL's file"
  (close (file tbl))
  (slot-makunbound tbl 'file))

(defun column-count (tbl)
  "Returns number of columns in TBL"
  (length (columns tbl)))

(defun record-count (tbl)
  "Returns number of records in TBL"
  (hash-table-count (records tbl)))

(defclass column ()
  ((name :initarg :name
         :reader name)
   (primary-key? :initarg :primary-key?
		 :initform nil
                 :reader primary-key?)))

(defun new-column (nam &rest opts)
  "Returns new columns for NAM and OPTS"
  (apply #'make-instance
         'column
         :name nam
	 opts))

(defun column-value (rec col)
  "Returns value for COL in REC"
  (rest (assoc col rec)))

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

(defun record-key (rec tbl)
  (mapcar (lambda (c)
            (column-value rec (name c)))
          (primary-key tbl)))
			     
(defmacro with-open-tables ((&rest tbls) &body body)
  (let (($tbl (gensym))
	($tbls (gensym)))
    `(let ((,$tbls (list ,@tbls)))
       (dolist (,$tbl ,$tbls)
	 (open-table ,$tbl))
       (unwind-protect
	    (progn ,@body)
	 (dolist (,$tbl ,$tbls)
	   (close-table ,$tbl))))))

(defmacro let-tables ((&rest tables) &body body)
  (labels ((bind (name &rest cols)
	     `(,name (new-table ',name
				,@(mapcar (lambda (c)
					    (if (listp c)
						`(new-column ',(first c) ,@(rest c))
						`(new-column ',c)))
					  cols)))))
    `(let (,@(mapcar (lambda (x) (apply #'bind x)) tables)) 
       ,@body)))

(defun write-value (out x)
  (write x :stream out))

(defun store-record (tbl rec)
  "Stores REC in TBL"
  (with-slots (file records) tbl
    (let* ((rec (remove-duplicates rec :key #'first :from-end t))
	   (key (record-key rec tbl)))
      (write-value file key)
      (write-value file rec)
      (terpri file)
      (setf (gethash key records) rec))))

(defun find-record (tbl &rest key)
  "Returns record for KEY in TBL if found, otherwise NIL"
  (gethash key (records tbl)))

(defun delete-record (tbl &rest key)
  "Deletes REC from TBL"
  (with-slots (file records) tbl
    (write-value file key)
    (write-value file *delete*)
    (terpri file)
    (remhash key records)))

(defun test-setup ()
  (when (probe-file "users.tbl")
    (assert (delete-file "users.tbl"))))

(defun table-tests ()
  (test-setup)
  
  (let-tables ((users (username :primary-key? t) password))
    (assert (string= (name users) 'users))
    (assert (= (column-count users) 2))
    (assert (eq (name (first (primary-key users))) 'username))
    (with-open-tables (users)
      (assert (= (record-count users) 0)))))

(defun record-tests ()
  (test-setup)
  
  (let-tables ((users (username :primary-key? t) password))
    (with-open-tables (users)
      (let ((rec (new-record 'username "ben_dover"
			     'password "badum")))
        (store-record users rec)
        (assert (string= (column-value (find-record users "ben_dover") 'password)
                         "badum"))
	
        (let ((rec (set-column-values rec 'password "dish")))
          (store-record users rec)
          (assert (string= (column-value (find-record users "ben_dover") 'password)
                           "dish")))

	(delete-record users "ben_dover")
	(assert (null (find-record users "ben_dover")))))))

(defun tests ()
  (table-tests)
  (record-tests))

