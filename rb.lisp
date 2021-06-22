(defpackage rb
  (:use cl)
  (:import-from util nor while)
  (:export clear compare
	   do-tree
	   find-node 
	   get-node
	   new-root node-count
	   remove-node root root-node
	   set-node
	   tests benchmarks))

(in-package rb)

;;(declaim (optimize (speed 3) (safety 0)))

(defmethod compare ((x fixnum) y)
  (declare (type fixnum x y))
  (cond
    ((< x y) :lt)
    ((> x y) :gt)
    (t :eq)))

(defmethod compare ((x string) y)
  (declare (type string x y))
  (cond
    ((string< x y) :lt)
    ((string> x y) :gt)
    (t :eq)))

(defmethod compare ((x symbol) y)
  (declare (type symbol x y))
  (compare (symbol-name x) (symbol-name y)))

(defmethod compare ((x list) y)
  (declare (type list x y))
  (cond
    ((and x (null y))
     :gt)
    ((and (null x) y)
     :lt)
    ((or (null x) (null y))
     :eq)
    (t
     (let ((res (compare (first x) (first y))))
       (if (eq res :eq)
	   (compare (rest x) (rest y))
	   res)))))

(defmacro do-tree ((key val root) &body body)
  (let (($done (gensym)) ($node (gensym)) ($todo (gensym)))
    `(let ((,$node ,root) ,$todo ,$done)
       (tagbody
	next
	  (unless ,$node (setf ,$node (pop ,$todo)))
	  
	  (when ,$node
	    (when (node-right ,$node)
	      (push (node-right ,$node) ,$todo))
	    (push ,$node ,$done)
	    (when (node-left ,$node)
	      (setf ,$node (node-left ,$node))
	      (go next))))
       
       (dolist (,$node ,$done)
	 (destructuring-bind ,key (node-key ,$node)
	   (let ((,val (node-value ,$node)))
	       ,@body))))))

(defstruct node
  (key nil :type t)
  (value nil :type t)
  (left nil :type (or node null))
  (right nil :type (or node null))
  (red? t :type boolean))

(defun new-node (key val)
  (make-node :key key :value val))

(defstruct root
  (compare nil :type function)
  (key nil :type function)
  (node nil :type (or node null))
  (size 0 :type fixnum))

(defun node-count (root)
  (root-size root))

(defmacro rotlf (node)
  `(setf ,node (rotl ,node)))

(defmacro rotrf (node)
  `(setf ,node (rotr ,node)))

(defun new-root (&key (compare #'compare) (key #'identity))
  (make-root :compare compare :key key))

(defun ccompare (root x y)
  (declare (type root root))
  (funcall (root-compare root) x y))

(defun ckey (root value)
  (declare (type root root))
  (funcall (root-key root) value))

(defun red? (node)
  (declare (type (or node null) node))
  (and node (node-red? node)))

(defun rotl (node)
  (declare (type node node))
  (let ((r (node-right node)))
    (setf (node-right node) (node-left r)
	  (node-left r) node
	  (node-red? r) (node-red? node)
	  (node-red? node) t)
    r))

(defun rotr (node)
  (declare (type node node))
  (let ((l (node-left node)))
    (setf (node-left node) (node-right l)
	  (node-right l) node
	  (node-red? l) (node-red? node)
	  (node-red? node) t)
    l))

(defun flip (node)
  (declare (type node node))
  (setf (node-red? node) (not (node-red? node))
	(node-red? (node-left node)) (not (node-red? (node-left node)))
	(node-red? (node-right node)) (not (node-red? (node-right node)))))

(defun fix (node)
  (declare (type node node))
  (when (red? (node-right node))
    (rotlf node))
  (when (and (red? (node-left node)) (red? (node-left (node-left node))))
    (rotrf node))
  (when (and (red? (node-left node)) (red? (node-right node)))
    (flip node))
  node)

(defun set-node (val root &key key)
  (declare (type root root))
  (let ((k (or key (ckey root val))))
    (labels ((rec (node)
	       (declare (type (or node null) node))
	       (if node
		   (progn
		     (ecase (ccompare root k (node-key node))
		       (:lt (multiple-value-bind (l ok) (rec (node-left node))
			      (setf (node-left node) l)
			      (values (fix node) ok)))
		       (:gt (multiple-value-bind (r ok) (rec (node-right node))
			      (setf (node-right node) r)
			      (values (fix node) ok)))
		       (:eq
			(setf (node-value node) val)
			(values node nil))))
		   (progn
		     (incf (root-size root))
		     (values (new-node k val) t)))))
      (multiple-value-bind (new-root ok) (rec (root-node root))
	(when ok
	  (setf (node-red? new-root) nil
		(root-node root) new-root)
	  t)))))

(defun clear (root)
  (setf (root-node root) nil (root-size root) 0))

(defmacro movef-red-left (node)
  `(setf ,node (move-red-left ,node)))

(defun move-red-left (node)
  (declare (type node node))
  (flip node)
  (when (red? (node-left (node-right node)))
    (rotrf (node-right node))
    (rotlf node)
    (flip node))
  node)

(defun remove-min (node)
  (declare (type node node))
  (if (null (node-left node))
      (values nil node)
      (progn
	(when (nor (red? (node-left node))
		   (red? (node-left (node-left node))))
	  (movef-red-left node))
	(multiple-value-bind (new-left new-node) (remove-min (node-left node))
	  (setf (node-left node) new-left)
	  (values (fix node) new-node)))))

(defun remove-node (key root)
  (declare (type root root))
  (labels ((rec (node)
	     (declare (type (or null node) node))
	     (if node
		 (if (eq (ccompare root key (node-key node)) :lt)
		     (progn
		       (when (nor (red? (node-left node))
				  (red? (node-left (node-left node))))
			 (movef-red-left node))
		       (multiple-value-bind (new-left val) (rec (node-left node))
			 (setf (node-left node) new-left)
			 (values (fix node) val)))
		     (progn
		       (when (red? (node-left node))
			 (rotrf node))
		       (if (and (eq (ccompare root key (node-key node)) :eq)
				(null (node-right node)))
			   (progn
			     (decf (root-size root))
			     (values nil (node-value node)))
			   (progn
			     (when (and (node-right node)
					(nor (red? (node-right node))
					     (red? (node-left (node-right node)))))
			       (flip node)
			       (when (red? (node-left (node-left node)))
				 (rotrf node)
				 (flip node)))
			     (if (eq (ccompare root key (node-key node)) :eq)
				 (progn
				   (let ((l (node-left node))
					 (val (node-value node)))
				     (multiple-value-bind (r new-node) (remove-min (node-right node))
				       (setf node new-node
					     (node-left node) l
					     (node-right node) r))
				     (decf (root-size root))
				     (values (fix node) val)))
				 (multiple-value-bind (new-right val) (rec (node-right node))
				   (setf (node-right node) new-right)
				   (values (fix node) val)))))))
		 (values node nil))))
    (multiple-value-bind (new-root val) (rec (root-node root))
      (when new-root
	(setf (node-red? new-root) nil))
      (setf (root-node root) new-root)
      val)))

(defun get-node (key root)
  (declare (type root root))
  (let ((node (root-node root)))
    (while node
      (ecase (ccompare root key (node-key node))
	(:lt
	 (setf node (node-left node)))
	(:gt
	 (setf node (node-right node)))
	(:eq
	 (return))))
    (and node (node-value node))))

(defun (setf get-node) (val key root)
  (set-node val root :key key))

(defun find-node (key root)
  (declare (type root root))
  (let ((node (root-node root)))
    (tagbody
     next
       (ecase (ccompare root key (node-key node))
	(:lt
	 (when (node-left node)
	   (setf node (node-left node))
	   (go next))
	 (:gt
	  (when (node-right node)
	    (setf node (node-right node))
	    (go next)))
	 (:eq))))
    node))

(defun tests ()
  (let ((root (new-root)))
    (assert (set-node 1 root))
    (assert (set-node 2 root))
    (assert (set-node 3 root))
    (assert (not (set-node 3 root)))
    (assert (set-node 4 root))
    (assert (= (remove-node 2 root) 2))
    (assert (null (remove-node 2 root)))
    (assert (= (node-count root) 3))
    (assert (= (get-node 1 root) 1))
    (assert (null (get-node 2 root)))
    (assert (= (get-node 3 root) 3))
    (assert (= (get-node 4 root) 4))))

(defun benchmarks ()
  (let ((max 1000000))
    (time
     (let ((tbl (make-hash-table)))
       (dotimes (i max)
	 (setf (gethash i tbl) i))
       (dotimes (i max)
	 (assert (= (gethash i tbl) i)))
       (dotimes (i max)
	 (assert (remhash i tbl)))))
    
    (time
     (let ((root (new-root)))
       (dotimes (i max)
	 (assert (set-node i root)))
       (dotimes (i max)
	 (assert (= (get-node i root) i)))
       (dotimes (i max)
	 (assert (= (remove-node i root) i)))))))
