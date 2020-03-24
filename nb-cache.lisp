(in-package #:caching)


(defparameter *max-clients* 100) 
;;Use Clack for websocket interaction to the front-caches

(defparameter *db* '())

(defparameter *setup-done* nil)

(defparameter *clients* '())

(defparameter *central-cache* nil)

(defparameter *numb-kernels* 8)

(defparameter *queue* nil)

		


(defparameter *max-clients* 100) 
;;Use Clack for websocket interaction to the front-caches

(defparameter *db* '())

(defparameter *setup-done* nil)

(defparameter *clients* '())

(defparameter *central-cache* nil)

(defparameter *numb-kernels* 8)

(defparameter *log-queue* nil)


(defun init ()
	(progn
		(setf *db* (loop for i from 0 below 10
									collect (cons i (* i 2))))))

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

(defmethod start-client ((client front-client) log-queue)
	(let ((resp-queue (:resp-q client))
				(cache-queue (:cache-req-q client))
				(req-queue (:client-req-q client)))
		(bt:make-thread
		 (lambda ()
			 (let ((go-on t))
				 (loop
						while (eql go-on t)
						do
							(sleep 0.05)
							(let ((req-data (queue-remove req-queue)))
								(when req-data
									(cond
										((eq req-data 'quit)
										 (setf go-on nil)
										 (let ((client-quit (make-instance 'client-quit :client-quit-id (:client-id client))))
											 (queue-add log-queue (format nil "CLient quit ~A from cache req queue ~A" client (local-time:now)))
											 (queue-add cache-queue client-quit)))
										((eq req-data 'flush)
										 (queue-add log-queue (format nil "Resp-q ~A Req-q ~A Client ~A" (:resp-q client) (:client-req-q client) (:client-id client))))
										(t
										 (progn
											 (queue-add log-queue (format nil "CLient req ~A from req queue. Client: ~A ~A" req-data (:client-id client) (local-time:now)))
											 (queue-add cache-queue req-data))))))
							(let ((resp-data (queue-remove resp-queue)))
								(when resp-data
									(cond
										((eq resp-data 'quit)
										 (setf go-on nil)
										 (let ((client-quit (make-instance 'client-quit :client-quit-id (:client-id client))))
											 (queue-add log-queue (format nil "CLient quit ~A from cache resp queue ~A" client (local-time:now)))
											 (queue-add cache-queue client-quit)))
										(t 
										 (queue-add log-queue (format nil "CLient got ~A from cache. Client ~A ~A" resp-data (:client-id client) (local-time:now)))))))))))))



(defun start-logger (log-queue)
	(let ((standard-out *standard-output*))
		(bt:make-thread
		 (lambda ()
			 (let ((go-on t))
				 (loop
						while (eql go-on t)
						do
							(sleep 0.05)
							(let ((logg-mess (queue-remove log-queue)))
								(when logg-mess
									(format standard-out "NBLOG: ~A~%" logg-mess)))))))))


(defgeneric handle-client-req (req client cache-data log-queue))


(defun handle-get-entry (req client cache-data log-queue)
	(let ((key (:key req)))
		(multiple-value-bind (val found)
				(gethash key cache-data)
			(if found
					(progn
						(queue-add (:resp-q client) (list val 'gethash)))
						;;(queue-add log-queue (format nil "Found in cache key: ~A val: ~A Client ~A" key val (:client-id client))))
					(let ((db-val (assoc key *db* :test #'=)))
						(if db-val
								(progn
									(queue-add (:resp-q client) (list db-val 'getdb))
									(setf (gethash key cache-data) db-val)
									;;(queue-add log-queue (format nil "Key found in db, Key: ~A Val: ~A Client: ~A" key db-val (:client-id client))))
									)
								(error "Key not found in db ~A" key)))))))

(defun handle-update-entry (req client cache-data log-queue)
	(let ((key (:key req)))
		(multiple-value-bind (val found)
				(gethash key cache-data)
			(if found
					(progn
						(remhash key cache-data)
						(db-update *db* key (:value req))
						;;(queue-add log-queue (format nil "UPDHASH: Key: ~A Value: ~A Found: ~A " key val found))
						(queue-add (:resp-q client) (list val 'updhash)))
					(progn
						(db-update *db* key (:value req))
						(queue-add (:resp-q client) (list (:value req) 'upddb)))))))
						;;(queue-add log-queue (format nil "UPDATE: Updated in db, ~A" key))))))

(defgeneric start-cache (cache log-queue))

(defmethod start-cache ((cache central-cache) log-queue)
	(bt:make-thread 
	 (lambda ()
		 (let ((go-on t)
					 (cache-data (:get-cache cache))
					 (req-queue (:req-q cache)))
			 (loop
					while (eql go-on t)
					do
						(let ((req-data (queue-remove req-queue)))
							(when req-data
								(cond ((eq req-data 'quit)
											 (setf go-on nil))
											((eq req-data 'flush)
											 (queue-add log-queue (format nil "Flush ~A" (:get-cache cache))))
											((typep req-data 'client-quit)
											 (let ((client-pred (get-client (:client-quit-id req-data))))
												 (queue-add log-queue (format nil "Remove client ~A~A" req-data (:client-quit-id req-data)))
												 (setf *clients* (remove-if client-pred *clients*))))
											((typep req-data 'front-client)
											 (let* ((client-pred (is-client-pred req-data))
															(client (find-if client-pred *clients*)))
												 (when (not client)
													 (progn
														 (queue-add log-queue (format nil "Client registered ~A CLientId: ~A" req-data (:client-id req-data)))
														 (cons req-data *clients*)))))
											((typep req-data 'cache-entry)
											 (if *clients*
													 (let ((client-p (get-client (:entry-client-id req-data))))											
														 (let ((client (find-if client-p *clients*)))
															 (if client
																	 (cond ((typep req-data 'get-entry)
																					(handle-get-entry req-data client cache-data log-queue))
																				 ((typep req-data 'update-entry)
																					(handle-update-entry req-data client cache-data log-queue)))
																	 ;;(handle-client-req req-data client cache-data log-queue)
																	 (error "Client not registered. Id: ~A ~A" (:entry-client-id req-data) *clients*))))
													 (error "No clients registered. ~A" *clients*)))
											(t (error "Not a known request ~A~%" req-data))))))))))



(defun setup-client (cache-queue)
	(let ((client (make-instance 'front-client
															 :client-id (gensym)
															 :resp-queue (make-atomic-queue 10)
															 :cache-req-queue cache-queue
															 :client-req-queue (make-atomic-queue 10))))
		(queue-add cache-queue client)
		(setf *clients* (cons client *clients*))
		client))

(defun setup-cache ()
	(progn
		(init)
		(setf *central-cache* (make-instance 'central-cache :req-queue (make-atomic-queue 10)))))
				 
		 
(defun shutdown ()
	(progn
		(loop for client in *clients*
			 do
				 (queue-add (:resp-q client) 'quit))
		(sleep 2)
		(queue-add (:req-q *central-cache*) 'quit)
		(setf *setup-done* nil)
		(setf *db* nil)))


(defun test ()
	(if (not *setup-done*)
			(progn
				(setf *log-queue* (make-atomic-queue 100))
				(start-logger *log-queue*)
				(progn
					(sleep 1)
					(setup-cache)
					(start-cache *central-cache* *log-queue*)
					(sleep 1)
					(loop for x below 4
						 do (setup-client (:req-q *central-cache*)))
					(loop for cl in *clients*
						 do (start-client cl *log-queue*)
							 (format t "Client started ~A~%" cl))
					(setf *setup-done* t)))
			(progn
				(loop for i from 0 below 4
					 do
						 (let* ((key (random (length *db*)))
										(client-num (random 4))
										(client (nth client-num *clients*))
										(entry (make-instance 'get-entry :entry-key key :entry-value (* key 2) :entry-client-id (:client-id client))))
							 (queue-add (:client-req-q client) entry)))
				(loop for i from 0 below 2
					 do
						 (let* ((key (random (length *db*)))
										(client-num (random 4))
										(client (nth client-num *clients*))
										(entry (make-instance 'update-entry :entry-key key :entry-value (* key 2) :entry-client-id (:client-id client))))
				(queue-add (:client-req-q client) entry))))))

(defun flush-clients ()
	(loop for cl in *clients*
		 do 
				(queue-add (:client-req-q cl) 'flush)))


(defun send-get-req (key value nth-client)
	(let ((client (nth nth-client *clients*)))
		(queue-add (:client-req-q client) (make-instance 'get-entry :entry-key key :entry-value value :entry-client-id (:client-id client)))))

(defun send-update-req (key value nth-client)
	(let ((client (nth nth-client *clients*)))
		(queue-add (:client-req-q client) (make-instance 'update-entry :entry-key key :entry-value value :entry-client-id (:client-id client)))))

(defun send-numb-get-req (numb)
	(loop for i from 0 below numb
		 do
			 (send-get-req i i (random 4))))

(defun send-numb-update-req (numb)
	(loop for i from 0 below numb
		 do
			 (send-update-req i i (random 4))))

(defun send-mixed-numb-upd-get (upto)
	(let ((gets (random upto))
				(upds (random upto)))
		(send-numb-get-req gets)
		(send-numb-update-req upds)
		(list gets upds)))
	
