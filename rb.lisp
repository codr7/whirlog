(defpackage rb
  (:use cl)
  (:import-from util nor while)
  (:shadow copy-tree)
  (:export add-node
	   clear
	   find-key
	   new-tree
	   remove-node
	   size
	   tree
	   tests benchmarks))

(in-package rb)

(declaim (optimize (speed 3) (safety 0)))

(defmethod compare ((x fixnum) (y fixnum))
  (declare (type fixnum x y))
  (cond
    ((< x y) :lt)
    ((> x y) :gt)
    (t :eq)))

(defstruct node
  (key nil :type t)
  (value nil :type t)
  (left nil :type (or node null))
  (right nil :type (or node null))
  (red? t :type boolean))

(defun new-node (key val)
  (make-node :key key :value val))

(defstruct tree
  (compare nil :type function)
  (key nil :type function)
  (root nil :type (or node null))
  (size 0 :type fixnum))

(defun size (tree)
  (tree-size tree))

(defmacro rotlf (node)
  `(setf ,node (rotl ,node)))

(defmacro rotrf (node)
  `(setf ,node (rotr ,node)))

(defun new-tree (&key (compare #'compare) (key #'identity))
  (make-tree :compare compare :key key))

(defun ccompare (tree x y)
  (declare (type tree tree))
  (funcall (tree-compare tree) x y))

(defun ckey (tree value)
  (declare (type tree tree))
  (funcall (tree-key tree) value))

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

(defun add-node (val tree &key key)
  (declare (type tree tree))
  (let ((k (or key (ckey tree val))))
    (labels ((rec (node)
	       (declare (type (or node null) node))
	       (if node
		   (progn
		     (ecase (ccompare tree k (node-key node))
		       (:lt (multiple-value-bind (l ok) (rec (node-left node))
			      (setf (node-left node) l)
			      (values (fix node) ok)))
		       (:gt (multiple-value-bind (r ok) (rec (node-right node))
			      (setf (node-right node) r)
			      (values (fix node) ok)))
		       (:eq (values node nil))))
		   (progn
		     (incf (tree-size tree))
		     (values (new-node k val) t)))))
      (multiple-value-bind (new-root ok) (rec (tree-root tree))
	(when ok
	  (setf (node-red? new-root) nil
		(tree-root tree) new-root)
	  t)))))

(defun clear (tree)
  (setf (tree-root tree) nil (tree-size tree) 0))
  
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

(defun remove-node (key tree)
  (declare (type tree tree))
  (labels ((rec (node)
	     (declare (type (or null node) node))
	     (if node
		 (if (eq (ccompare tree key (node-key node)) :lt)
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
		       (if (and (eq (ccompare tree key (node-key node)) :eq)
				(null (node-right node)))
			   (progn
			     (decf (tree-size tree))
			     (values nil (node-value node)))
			   (progn
			     (when (and (node-right node)
					(nor (red? (node-right node))
					     (red? (node-left (node-right node)))))
			       (flip node)
			       (when (red? (node-left (node-left node)))
				 (rotrf node)
				 (flip node)))
			     (if (eq (ccompare tree key (node-key node)) :eq)
				 (progn
				   (let ((l (node-left node))
					 (val (node-value node)))
				     (multiple-value-bind (r new-node) (remove-min (node-right node))
				       (setf node new-node
					     (node-left node) l
					     (node-right node) r))
				     (decf (tree-size tree))
				     (values (fix node) val)))
				 (multiple-value-bind (new-right val) (rec (node-right node))
				   (setf (node-right node) new-right)
				   (values (fix node) val)))))))
		 (values node nil))))
    (multiple-value-bind (new-root val) (rec (tree-root tree))
      (when new-root
	(setf (node-red? new-root) nil))
      (setf (tree-root tree) new-root)
      val)))

(defun find-key (key tree)
  (declare (type tree tree))
  (let ((node (tree-root tree)))
    (while node
      (ecase (ccompare tree key (node-key node))
	(:lt
	 (setf node (node-left node)))
	(:gt
	 (setf node (node-right node)))
	(:eq
	 (return))))
    (and node (node-value node))))

(defun tests ()
  (let ((tree (new-tree)))
    (assert (add-node 1 tree))
    (assert (add-node 2 tree))
    (assert (add-node 3 tree))
    (assert (not (add-node 3 tree)))
    (assert (add-node 4 tree))
    (assert (= (remove-node 2 tree) 2))
    (assert (null (remove-node 2 tree)))
    (assert (= (size tree) 3))
    (assert (= (find-key 1 tree) 1))
    (assert (null (find-key 2 tree)))
    (assert (= (find-key 3 tree) 3))
    (assert (= (find-key 4 tree) 4))))

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
     (let ((tree (new-tree)))
       (dotimes (i max)
	 (assert (add-node i tree)))
       (dotimes (i max)
	 (assert (= (find-key i tree) i)))
       (dotimes (i max)
	 (assert (= (remove-node i tree) i)))))))
