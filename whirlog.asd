(asdf:defsystem whirlog
  :name "whirlog"
  :version "1"
  :maintainer "codr7"
  :author "codr7"
  :description "a simple, log-based relational database"
  :licence "MIT"
  :serial t
  :components ((:file "util")
               (:file "rb")
               (:file "whirlog")))
