;;;; caching.lisp

(in-package #:caching)


(defparameter *max-clients* 100) 
;;Use Clack for websocket interaction to the front-caches

(defparameter *db* '())

(defparameter *setup-done* nil)

(defparameter *clients* '())

(defparameter *central-cache* nil)

(defparameter *numb-kernels* 8)

(defun init ()
	(progn
		(setf *db* (loop for i from 0 below 1000
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

(defgeneric start-client (client channel log-queue))

(defmethod start-client ((client front-client) channel log-queue)
	(submit-task
	 channel
	 (lambda ()
		 (let ((go-on t)
					 (req-queue (:client-req-q client))
					 (cache-queue (:cache-req-q client))
					 (resp-queue (:resp-q client)))
			 (loop
					while (eql go-on t)
					do (progn
							 (when (not (queue-empty-p req-queue))
								 (let ((req-data (pop-queue req-queue)))
									 (cond
										 ((eq req-data 'quit)
											(setf go-on nil)
											(let ((client-quit (make-instance 'client-quit :client-quit-id (:client-id client))))
												(push-queue (format nil "CLient quit ~A from cache req queue" client) log-queue)
												(push-queue client-quit cache-queue)))
										 (t
											(push-queue req-data cache-queue)))))
							 (when (not (queue-empty-p resp-queue))
								 (let ((resp-data (pop-queue resp-queue)))
									 (cond
										 ((eq resp-data 'quit)
											(setf go-on nil)
											(let ((client-quit (make-instance 'client-quit :client-quit-id (:client-id client))))
												(push-queue (format nil "CLient quit ~A from cache resp queue" client) log-queue)
												(push-queue client-quit cache-queue)))
										 (t 
											(push-queue (format nil "CLient got ~A from cache" resp-data) log-queue)))))))))))

(defun start-logger (log-queue)
	(let ((standard-out *standard-output*))
		(submit-task
		 (make-channel)
		 (lambda ()
			 (labels ((rec ()
									(let ((logg-mess (pop-queue log-queue)))
										(format standard-out "LOG: ~A~%" logg-mess)
										(rec))))
				 (rec))))))

(defgeneric handle-client-req (req client cache-data log-queue))

(defmethod handle-client-req ((req get-entry) client cache-data log-queue)
	(let ((key (:key req)))
		(multiple-value-bind (val found)
				(gethash key cache-data)
			(if found
					(progn
						(remhash key cache-data)
						(push-queue (format nil "Found in cache ~A" key) log-queue))
					(let ((db-val (assoc key *db* :test #'=)))
						(if db-val
								(progn
									(push-queue db-val (:resp-q client))
									(setf (gethash key cache-data) db-val)
									(push-queue (format nil "Key found in db, ~A" key) log-queue))
								(error "Key not found in db ~A" key)))))))

(defmethod handle-client-req ((req update-entry) client cache-data log-queue)
	(let ((key (:key req)))
		(multiple-value-bind (val found)
				(gethash key cache-data)
			(if found
					(progn
						(remhash key cache-data)
						(db-update *db* key (:value req))
						(push-queue (format nil "Removed in cache ~A. Updated in db" key) log-queue)
						(push-queue val (:resp-q client)))
					(progn
						(db-update *db* key (:value req))
						(push-queue req (:resp-q client))
						(push-queue (format nil "Updated in db, ~A" key) log-queue))))))
					
	
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
						do (when (not (queue-empty-p req-queue))
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
																(push-queue (format nil "Client registered ~A" req-data) log-queue)
																(cons req-data *clients*)))))
												 ((typep req-data 'cache-entry)
													(if *clients*
															(let ((client-p (get-client (:entry-client-id req-data))))											
																(let ((client (find-if client-p *clients*)))
																	(if client
																			(handle-client-req req-data client cache-data log-queue)
																			(error "Client not registered. Id: ~A ~A" (:entry-client-id req-data) *clients*))))
															(error "No clients registered. ~A" *clients*)))
												 (t (error "Not a known request ~A~%" req-data))))))))))



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
		(end-kernel :wait t)))



(defun test ()
	(if (not *setup-done*)
			(let ((log-queue (make-queue)))
				(start-logger log-queue)
				(progn
					(sleep 1)
					(setup-cache)
					(start-cache *central-cache* (make-channel) log-queue)
					(sleep 1)
					(loop for x below 22
						 do (setup-client (:req-q *central-cache*)))
					(loop for cl in *clients*
						 do (start-client cl (make-channel) log-queue)
							 (format t "Client started ~A~%" cl))
					(setf *setup-done* t)
					(sleep 1)))
			(progn
				(loop for i below 20
					 do
						 (let* ((key (random 20))
										(client-num (random 4))
										(client (nth client-num *clients*))
										(entry (make-instance 'get-entry :entry-key key :entry-value (* key 2) :entry-client-id (:client-id client))))
							 (push-queue entry (:client-req-q client))))
				(loop for i from 0 below 10
					 do
						 (let* ((key (random 20))
										(client-num (random 4))
										(client (nth client-num *clients*))
										(entry (make-instance 'update-entry :entry-key key :entry-value (* key 2) :entry-client-id (:client-id client))))
							 (push-queue entry (:client-req-q client)))))))
	
	
			
