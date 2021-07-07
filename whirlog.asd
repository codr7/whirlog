(asdf:defsystem whirlog
  :name "whirlog"
  :version "1"
  :maintainer "codr7"
  :author "codr7"
  :description "a minimal versioned log structured relational DB"
  :licence "MIT"
  :serial t
  :components ((:file "util")
	       (:file "sort")
               (:file "rb")
	       (:file "lset")
               (:file "whirlog")))
