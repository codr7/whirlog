(asdf:defsystem whirlog
  :name "whirlog"
  :version "1"
  :maintainer "codr7"
  :author "codr7"
  :description "A minimalistic versioned, log-based relational database."
  :licence "MIT"
  :serial t
  :components ((:file "util")
               (:file "rb")
               (:file "whirlog")))
