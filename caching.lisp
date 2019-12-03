;;;; caching.lisp

(in-package #:caching)



;;Use Clack for websocket interaction to the front-caches


(setf lparallel:*kernel* (lparallel:make-kernel 4))

(defclass cache-entry ()
	((resp-queue
		:initarg :resp-queue
		:accessor :resp-q
		:initform (error "you didn't supply an initial value for slot resp-queue"))
	 (entry-key 
		:initarg :entry-key
		:accessor :key
		:initform (error "you didn't supply an initial value for slot entry-key"))
	 (entry-value
		:initarg :entry-value
		:accessor :value
		:initform (error "you didn't supply an initial value for slot entry-value"))))
	
(defclass central-cache ()
	((req-queue
		:initarg :req-queue
		:accessor :req-q
		:initform (error "you didn't supply an initial value for slot rec-queue"))
	 (cache-data
		:initarg :cache
		:accessor :get-cache
		:initform (make-hash-table))))


(defgeneric start-cache (cache))

(defmethod start-cache ((cache central-cache))
	(let ((go-on t)
				(cache-data (:get-cache cache))
				(req-queue (:req-q cache)))
		(loop
			 while (eql go-on t)
			 do (when (not (queue-empty-p req-queue))
					 (let ((req-data (pop-queue req-queue)))
						 (cond ((eq req-data 'quit)
										(setf go-on nil))
									 ((eq req-data 'flush)
										(format t "Flush ~A~%" (:get-cache cache)))
									 ((typep req-data 'cache-entry)
										(multiple-value-bind (val found)
												(gethash (:key req-data) cache-data)
											(if found
													(progn
														(push-queue val (:resp-q req-data))
														(format t "Found ~A~%" (:key req-data)))
													(format t "Not found ~A~%" (:key req-data)))))
									 (t (format t "Not a known request ~A~%" req-data))))))))
				 
				 
				 
(defun test-sync ()
	(let ((req-queue (make-queue))
				(entry1-queue (make-queue))
				(channel (make-channel)))
		(let ((central-cache (make-instance 'central-cache :req-queue req-queue))
					(entry1 (make-instance 'cache-entry :entry-key "Bla" :entry-value "BlaVal" :resp-queue entry1-queue)))
			(setf (gethash "Bla" (:get-cache central-cache)) "BlaValue")
			(submit-task channel (lambda () (start-cache central-cache)))
			(push-queue entry1 req-queue)
			(format t "Resp val ~A" (pop-queue entry1-queue))
			(push-queue 'quit req-queue))))
		
				 
		 
	
	

(defun test ()
	(let ((chan (lparallel:make-channel)))
		(lparallel:submit-task chan '+ 3 4)
		(lparallel:submit-task chan (lambda () (+ 5 6)))
		(list (lparallel:receive-result chan)
					(lparallel:receive-result chan))))

(defun test1 ()
	(let ((queue (make-queue))
				(channel (make-channel)))
		(submit-task channel (lambda () (list (pop-queue queue)
																		 (pop-queue queue))))
		(push-queue "hello" queue)
		(push-queue "world" queue)
		(receive-result channel)))
	


