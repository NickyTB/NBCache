;;;; caching.lisp

(in-package #:caching)



;;Use Clack for websocket interaction to the front-caches


(setf lparallel:*kernel* (lparallel:make-kernel 4))

(defclass front-cache ()
	((sync-queue
		:initarg :sync-queue
		:accessor :sync-q
		:initform (error "you didn't supply an initial value for slot sync-queue"))
	 (req-queue
		:initarg :req-queue
		:accessor :req-q
		:initform (error "you didn't supply an initial value for slot rec-queue"))
	 (notify-queue
		:initarg :notify-queue
		:accessor :not-q
		:initform (error "you didn't supply an initial value for slot rnot-queue"))
	 (cache-data
		:initarg :cache
		:accessor :get-cache
		:initform '())))


(defun blip ()
		'test)


(defgeneric start-cache (cache))

(defmethod start-cache ((cache front-cache))
	(let ((go-on t)
				(sync-channel (lparallel:make-channel))
				(req-channel (lparallel:make-channel))
				(sync (:sync-q cache))
				(cache-data (:get-cache cache)))
		(loop
			 while (eql go-on t)
			 do (when (not (queue-empty-p sync))
					 (let ((sync-data (pop-queue sync)))
						 (cond ((eq sync-data 'quit)
										(setf go-on nil))
									 ((eq sync-data 'flush)
										(format t "Flush ~A~%" (:get-cache cache)))
									 (t (setf (:get-cache cache) (cons sync-data (:get-cache cache))))))))))
				 
				 
				 
(defun test-sync ()
	(let ((sync-queue (make-queue))
				(req-queue (make-queue))
				(not-queue (make-queue))
				(channel (make-channel)))
		(let ((front-cache (make-instance 'front-cache :sync-queue sync-queue :req-queue req-queue :notify-queue not-queue)))
			(submit-task channel (lambda () (start-cache front-cache)))
			(push-queue 'test1 sync-queue)
			(push-queue 'test2 sync-queue)
			(push-queue 'test3 sync-queue)
			(push-queue 'flush sync-queue)
			(push-queue 'quit sync-queue))))
		
				 
		 
	
	

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
	


