(in-package #:caching)


(defparameter *max-clients* 100) 
;;Use Clack for websocket interaction to the front-caches

(defparameter *db* '())

(defparameter *setup-done* nil)

(defparameter *clients* '())

(defparameter *central-cache* nil)

(defparameter *numb-kernels* 8)

(defstruct nb-atomic-queue queue)
	
(defun init ()
	(progn
		(setf *db* (loop for i from 0 below 10
									collect (cons i (* i 2))))
		(setf lparallel:*kernel* (lparallel:make-kernel *numb-kernels*)))) 

(defstruct entry id key value)
			 
(defun make-atomic-queue (size)
 (make-nb-atomic-queue :queue (make-array size :element-type 'entry)))

(defun make-queue (size)
	(make-array size :element-type 'entry))

(defmethod test ((in entry))
	"entry")

(defmethod test ((in integer))
	"int")

(defmethod test ((in atomic-queue))
	"queue")

(defvar *queue* (make-atomic-queue 10))

(defun update-queue (queue idx val)
	(let* ((q (nb-atomic-queue-queue queue))
				 (old (svref q idx)))
			(loop repeat 20 do
					 (if (eq (compare-and-swap (svref q idx) old val) old)
							 (return val)
							 (sleep 0.005)))))
			
	
(defstruct foo list)
  (defvar *foo* (make-foo))
  (atomic-update (foo-list *foo*) #'cons t)
	


