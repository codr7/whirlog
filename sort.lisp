(defpackage sort
  (:use cl)
  (:export compare))

(in-package sort)

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
