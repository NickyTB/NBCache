;;;; caching.lisp

(in-package #:caching)


(defparameter *max-clients* 100) 
;;Use Clack for websocket interaction to the front-caches


(setf lparallel:*kernel* (lparallel:make-kernel 4))

(defclass cache-entry ()
	((id 
		:initarg :entry-client-id
		:accessor :entry-client-id
		:initform (error "you didn't supply an initial value for slot id"))
	 (entry-key 
		:initarg :entry-key
		:accessor :key
		:initform (error "you didn't supply an initial value for slot entry-key"))
	 (entry-value
		:initarg :entry-value
		:accessor :value
		:initform (error "you didn't supply an initial value for slot entry-value"))))

(defclass client ()
	((id
		:initarg :client-id
		:accessor :client-id
		:initform (error "you didn't supply an initial value for slot id"))
	 (resp-queue
		:initarg :resp-queue
		:accessor :resp-q
		:initform (error "you didn't supply an initial value for slot resp-queue"))))
	
(defclass central-cache ()
	((req-queue
		:initarg :req-queue
		:accessor :req-q
		:initform (error "you didn't supply an initial value for slot rec-queue"))
	 (cache-data
		:initarg :cache
		:accessor :get-cache
		:initform (make-hash-table))))

(defun is-client-pred (client)
	(lambda (id) (eq id (:client-id client))))

(defun get-client (id)
	(lambda (client) (eq id (:client-id client))))



(defgeneric start-cache (cache))

(defmethod start-cache ((cache central-cache))
	(let ((go-on t)
				(cache-data (:get-cache cache))
				(req-queue (:req-q cache))
				(clients (make-array *max-clients* :element-type 'client))
				(index 0))
		(loop
			 while (eql go-on t)
			 do (when (not (queue-empty-p req-queue))
						(let ((req-data (pop-queue req-queue)))
						 (cond ((eq req-data 'quit)
										(setf go-on nil))
									 ((eq req-data 'flush)
										(format t "Flush ~A~%" (:get-cache cache)))
									 ((typep req-data 'client)
										(let* ((client-pred (is-client-pred req-data))
													 (client (find-if client-pred clients)))
											(when (not client)
												(progn
													(setf (aref clients index) req-data)
													(1+ index)))))
									 ((typep req-data 'cache-entry)
										(if (typep (aref clients 0) 'client)
												(let ((client-p (get-client (:entry-client-id req-data))))											
													(let ((client (find-if client-p clients)))
														(if client
																(multiple-value-bind (val found)
																		(gethash (:key req-data) cache-data)
																	(if found
																			(progn
																				(push-queue val (:resp-q client))
																				(format t "Found ~A~%" (:key req-data)))
																			(error "Not found ~A~%" (:key req-data))))
																(error "Client not registered. Id: ~A" (:entry-client-id req-data)))))
												(error "No clients registered. ~A" clients)))
										(t (error "Not a known request ~A~%" req-data))))))))
	
				 
				 
(defun test-sync ()
	(let ((req-queue (make-queue))
				(channel (make-channel)))
		(let* ((central-cache (make-instance 'central-cache :req-queue req-queue))
					 (client-id (gensym))
					 (entry1 (make-instance 'cache-entry :entry-key "Bla" :entry-value "BlaVal" :entry-client-id client-id))
					 (client (make-instance 'client :client-id client-id :resp-queue (make-queue))))
			(setf (gethash "Bla" (:get-cache central-cache)) "BlaValue")
			(submit-task channel (lambda () (start-cache central-cache)))
			(push-queue client req-queue)
			(push-queue entry1 req-queue)
			(format t "Resp val ~A" (pop-queue (:resp-q client)))
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
	


