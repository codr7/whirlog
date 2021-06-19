(asdf:defsystem whirlog
  :name "whirlog"
  :version "1"
  :maintainer "codr7"
  :author "codr7"
  :description "A minimalistic single process, mt capable, versioned, log structured relational database."
  :licence "MIT"
  :serial t
  :components ((:file "util")
               (:file "rb")
               (:file "whirlog")))
