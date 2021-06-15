(defpackage util
  (:use cl)
  (:export dohash let-when nor while
	   get-kw kw sethash sym))

(in-package util)

(defmacro dohash ((k v tbl) &body body)
  (let (($i (gensym)) ($k (gensym)) ($next (gensym)) ($ok (gensym)))
    `(with-hash-table-iterator (,$i ,tbl)
       (tagbody
	  ,$next
	  (multiple-value-bind (,$ok ,$k ,v) (,$i)
	    (when ,$ok
	      (destructuring-bind ,k ,$k
		,@body
		(go ,$next))))))))

(defmacro let-when (var form &body body)
  `(let ((,var ,form))
     (when ,var
       ,@body)))

(defmacro nor (&rest args)
  `(not (or ,@args)))

(defmacro while (cnd &body body)
  (let (($next (gensym)))
    `(block nil
       (tagbody
	  ,$next
	  (when ,cnd
	    ,@body
	    (go ,$next))))))

(defun get-kw (kw lst)
  (second (member kw lst)))

(defun kw (&rest args)
  (intern (with-output-to-string (out)
	    (dolist (a args)
	      (princ (if (stringp a) (string-upcase a) a) out)))
	  :keyword))

(defun sethash (key tbl val)
  (setf (gethash key tbl) val))

(defun sym (&rest args)
  (intern (with-output-to-string (out)
	    (dolist (a args)
	            (princ (if (stringp a) (string-upcase a) a) out)))))
