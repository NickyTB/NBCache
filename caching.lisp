;;;; caching.lisp

(in-package #:caching)


(defparameter *max-clients* 100) 
;;Use Clack for websocket interaction to the front-caches

(defparameter *db* '())

(defparameter *setup-done* nil)

(defparameter *clients* '())

(defparameter *central-cache* nil)

(defparameter *numb-kernels* 10)


(defun init ()
	(progn
		(setf *db* (loop for i from 0 below 10
									collect (cons i (* i 2))))
		(setf lparallel:*kernel* (lparallel:make-kernel *numb-kernels*)))) 

(defclass client-quit ()
	((id 
		:initarg :client-quit-id
		:accessor :client-quit-id
		:initform (error "you didn't supply an initial value for slot id"))))

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

(defclass get-entry (cache-entry)
	())
(defclass update-entry (cache-entry)
	())


(defclass front-client ()
	((id
		:initarg :client-id
		:accessor :client-id
		:initform (error "you didn't supply an initial value for slot id"))
	 (req-queue
		:initarg :client-req-queue
		:accessor :client-req-q
		:initform (error "you didn't supply an initial value for slot req-queue"))
	 (cache-req-queue
		:initarg :cache-req-queue
		:accessor :cache-req-q
		:initform (error "you didn't supply an initial value for slot cache-req-queue"))
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
	(lambda (client)
		(eq id (:client-id client))))
	

(defun db-update (db key value)
  (let ((cell (assoc key db :test #'=)))
    (if cell
        (setf (cdr cell) value)
        (setf db (acons key value db)))))


(defgeneric start-client (client log-queue))

(defgeneric start-client1 (client log-queue))


(defmethod start-client ((client front-client) log-queue)
	(let ((resp-queue (:resp-q client))
				(cache-queue (:cache-req-q client))
				(req-queue (:client-req-q client)))
		(submit-task
		 (make-channel)
		 (lambda ()
			 (let ((go-on t))
				 (loop
						while (eql go-on t)
						do
							(when (not (queue-empty-p req-queue))
								(let ((req-data (pop-queue req-queue)))
									(cond
										((eq req-data 'quit)
										 (setf go-on nil)
										 (let ((client-quit (make-instance 'client-quit :client-quit-id (:client-id client))))
											 (push-queue (format nil "CLient quit ~A from cache req queue" client) log-queue)
											 (push-queue client-quit cache-queue)))
										(t
										 (progn
											 (push-queue (format nil "CLient req ~A from req queue. Client: ~A" req-data (:client-id client)) log-queue)
											 (push-queue req-data cache-queue))))))
							(when (not (queue-empty-p resp-queue))
								(let ((resp-data (pop-queue resp-queue)))
									(cond
										((eq resp-data 'quit)
										 (setf go-on nil)
										 (let ((client-quit (make-instance 'client-quit :client-quit-id (:client-id client))))
											 (push-queue (format nil "CLient quit ~A from cache resp queue" client) log-queue)
											 (push-queue client-quit cache-queue)))
										(t 
										 (push-queue (format nil "CLient got ~A from cache. Client ~A" resp-data (:client-id client)) log-queue)))))))))))


(defun start-logger (log-queue)
	(let ((standard-out *standard-output*))
		(submit-task
		 (make-channel)
		 (lambda ()
			 (let ((*standard-output* standard-out))
				 (labels ((rec ()
										(let ((logg-mess (pop-queue log-queue)))
											(format standard-out "LOG: ~A~%" logg-mess)
											(rec))))
					 (rec)))))))


(defun start-logger1 (log-queue)
	(let ((standard-out *standard-output*))
		(submit-task
		 (make-channel)
		 (lambda ()
			 (let ((go-on t))
				 (loop
						while (eql go-on t)
						do
							(let ((logg-mess (pop-queue log-queue)))
								(format standard-out "LOG: ~A~%" logg-mess))))))))
							

(defgeneric handle-client-req (req client cache-data log-queue))

(defmethod handle-client-req ((req get-entry) client cache-data log-queue)
	(let ((key (:key req)))
		(multiple-value-bind (val found)
				(gethash key cache-data)
			(if found
					(progn
						(push-queue val (:resp-q client))
						(push-queue (format nil "Found in cache key: ~A val: ~A Client ~A" key val (:client-id client)) log-queue))
					(let ((db-val (assoc key *db* :test #'=)))
						(if db-val
								(progn
									(push-queue db-val (:resp-q client))
									(setf (gethash key cache-data) db-val)
									(push-queue (format nil "Key found in db, Key: ~A Val: ~A Client: ~A" key db-val (:client-id client)) log-queue))
								(error "Key not found in db ~A" key)))))))

(defmethod handle-client-req ((req update-entry) client cache-data log-queue)
	(let ((key (:key req)))
		(multiple-value-bind (val found)
				(gethash key cache-data)
			(if found
					(progn
						(remhash key cache-data)
						(db-update *db* key (:value req))
						(push-queue (format nil "UPDATE: Removed in cache ~A. Updated in db" key) log-queue))
						;;(push-queue val (:resp-q client)))
					(progn
						(db-update *db* key (:value req))
						;;(push-queue (:value req) (:resp-q client))
						(push-queue (format nil "UPDATE: Updated in db, ~A" key) log-queue))))))
					
	
(defgeneric start-cache (cache channel log-queue))

(defmethod start-cache ((cache central-cache) channel log-queue)
	(submit-task
		 channel
		 (lambda ()
			 (let ((go-on t)
						 (cache-data (:get-cache cache))
						 (req-queue (:req-q cache)))
				 (loop
						while (eql go-on t)
						do
							(let ((req-data (pop-queue req-queue)))
								(cond ((eq req-data 'quit)
											 (setf go-on nil))
											((eq req-data 'flush)
											 (push-queue (format nil "Flush ~A" (:get-cache cache)) log-queue))
											((typep req-data 'client-quit)
											 (let ((client-pred (get-client (:client-quit-id req-data))))
												 (push-queue (format nil "Remove client ~A~A" req-data (:client-quit-id req-data)) log-queue)
												 (setf *clients* (remove-if client-pred *clients*))))
											((typep req-data 'front-client)
											 (let* ((client-pred (is-client-pred req-data))
															(client (find-if client-pred *clients*)))
												 (when (not client)
													 (progn
														 (push-queue (format nil "Client registered ~A CLientId: ~A" req-data (:client-id req-data)) log-queue)
														 (cons req-data *clients*)))))
											((typep req-data 'cache-entry)
											 (if *clients*
													 (let ((client-p (get-client (:entry-client-id req-data))))											
														 (let ((client (find-if client-p *clients*)))
															 (if client
																	 (handle-client-req req-data client cache-data log-queue)
																	 (error "Client not registered. Id: ~A ~A" (:entry-client-id req-data) *clients*))))
													 (error "No clients registered. ~A" *clients*)))
											(t (error "Not a known request ~A~%" req-data)))))))))



(defun setup-client (cache-queue)
	(let ((client (make-instance 'front-client
															 :client-id (gensym)
															 :resp-queue (make-queue)
															 :cache-req-queue cache-queue
															 :client-req-queue (make-queue))))
		(push-queue client cache-queue)
		(setf *clients* (cons client *clients*))
		client))

(defun setup-cache ()
	(progn
		(init)
		(setf *central-cache* (make-instance 'central-cache :req-queue (make-queue)))))
				 
		 
(defun shutdown ()
	(progn
		(loop for client in *clients*
			 do
				 (push-queue 'quit (:resp-q client)))
		(sleep 2)
		(push-queue 'quit (:req-q *central-cache*))
		(end-kernel :wait nil)
		(setf *setup-done* nil)
		(setf *db* nil)))

(defun send-response-get (key &key (client nil))
	(let ((entry (make-instance 'get-entry :entry-key key :entry-value nil :entry-client-id (:client-id client))))
		(push-queue entry (:client-req-q client))))

(defun send-response-update (key value &key (client nil))
	(let ((entry (make-instance 'update-entry :entry-key key :entry-value value :entry-client-id (:client-id client))))
		(push-queue entry (:client-req-q client))))


(defun test ()
	(if (not *setup-done*)
			(let ((log-queue (make-queue)))
				(start-logger1 log-queue)
				(progn
					(sleep 1)
					(setup-cache)
					(start-cache *central-cache* (make-channel) log-queue)
					(sleep 1)
					(loop for x below 4
						 do (setup-client (:req-q *central-cache*)))
					(loop for cl in *clients*
						 do (start-client cl log-queue)
							 (format t "Client started ~A id ~A~%" cl (:client-id cl)))
					(setf *setup-done* t)))
			(progn
				(loop for i from 0 below 4
					 do
						 (let* ((key (random (length *db*)))
										(client-num (random 4))
										(client (nth client-num *clients*))
										(entry (make-instance 'get-entry :entry-key key :entry-value (* key 2) :entry-client-id (:client-id client))))
							 (push-queue entry (:client-req-q client))))
				(loop for i from 0 below 5
					 do
						 (let* ((key (random (length *db*)))
										(client-num (random 4))
										(client (nth client-num *clients*))
										(entry (make-instance 'update-entry :entry-key key :entry-value (* key 2) :entry-client-id (:client-id client))))
							 (push-queue entry (:client-req-q client)))))))

(defun send-get-req (key value nth-client)
	(let ((client (nth nth-client *clients*)))
		(push-queue (make-instance 'get-entry :entry-key key :entry-value value :entry-client-id (:client-id client)) (:client-req-q client))))

(defun send-update-req (key value nth-client)
	(let ((client (nth nth-client *clients*)))
		(push-queue (make-instance 'update-entry :entry-key key :entry-value value :entry-client-id (:client-id client)) (:client-req-q client))))

(defun send-numb-get-req (numb)
	(loop for i from 0 below numb
		 do
			 (send-get-req i i (random 4))))

(defun send-numb-update-req (numb)
	(loop for i from 0 below numb
		 do
			 (send-update-req i i (random 4))))
	
	
			
