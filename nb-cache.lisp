(in-package #:caching)


(defparameter *max-clients* 100) 
;;Use Clack for websocket interaction to the front-caches

(defparameter *db* '())

(defparameter *setup-done* nil)

(defparameter *clients* '())

(defparameter *central-cache* nil)

(defparameter *numb-kernels* 8)

(defparameter *queue* nil)

(defstruct nb-atomic-queue queue (first 0 :type (unsigned-byte 64)) (last 0 :type (unsigned-byte 64)) (size 0 :type (unsigned-byte 64)) (cap 0 :type  (unsigned-byte 64)))
	
(defun init ()
	(setf *db* (loop for i from 0 below 10
								collect (cons i (* i 2)))))
		

(defstruct entry id key value)
			 
(defun make-atomic-queue (size)
	(make-nb-atomic-queue :queue (make-array size :element-type 'entry) :cap size))

(defun init-queue ()
	(setf *queue* (make-atomic-queue 10)))


(defun queue-full-p (queue)
	(let ((fst-idx (nb-atomic-queue-first queue)))
		(or
		 (and
			(eql 0 fst-idx)
			(eql (nb-atomic-queue-cap queue) (nb-atomic-queue-last queue)))
		 (eql (nb-atomic-queue-last queue) fst-idx))))
	
(defun queue-empty-p (queue)
	(if (eql (nb-atomic-queue-first queue) (nb-atomic-queue-last queue))
			t
			nil))

(defun incf-queue (queue)
	(if (eql (nb-atomic-queue-last queue) (- (nb-atomic-queue-cap queue) 1))
			(let ((q-last (nb-atomic-queue-last queue))
						(q-new-last 0))
				(loop repeat 20 do
						 (if (eq (sb-lockless::compare-and-swap (nb-atomic-queue-last queue) q-last q-new-last) q-last)
								 (return q-new-last)
								 (sleep 0.005))))
			(sb-lockless::atomic-incf (nb-atomic-queue-last queue))))
	
(defun queue-add (queue entry)
	(if (queue-full-p queue)
			nil
			(progn
				(update-queue queue (nb-atomic-queue-last queue) entry)
				(incf-queue queue)
				entry)))


	
(defun update-queue (queue idx val)
	(let* ((q (nb-atomic-queue-queue queue))
				 (old (svref q idx)))
			(loop repeat 20 do
					 (if (eq (sb-lockless::compare-and-swap (svref q idx) old val) old)
							 (return val)
							 (sleep 0.005)))))
			
	


