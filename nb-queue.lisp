(in-package #:caching)

(defparameter *queue* nil)

(defvar *lock* (bt:make-lock))

(defstruct nb-atomic-queue queue (first 0 :type (unsigned-byte 64)) (last 0 :type (unsigned-byte 64)) (size 0 :type (unsigned-byte 64)) (cap 0 :type  (unsigned-byte 64)))		

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
		 (and
			(eql (nb-atomic-queue-last queue) fst-idx)
			(not (eql (nb-atomic-queue-size queue) 0))))))
	
(defun queue-empty-p (queue)
	(if (eql (nb-atomic-queue-size queue) 0)
			t
			nil))

(defun queue-grow (queue)
	(bt:with-lock-held (*lock*)
		(if (eql (nb-atomic-queue-last queue) (- (nb-atomic-queue-cap queue) 1))
				(progn
					(setf (nb-atomic-queue-last queue) 0)
					(incf  (nb-atomic-queue-size queue))
					0)
				(progn
					(incf (nb-atomic-queue-size queue))
					(incf (nb-atomic-queue-last queue))))))
						  

(defun queue-shrink (queue)
	(bt:with-lock-held (*lock*)
		(if (eql (nb-atomic-queue-first queue) (- (nb-atomic-queue-cap queue) 1))
				(progn
					(setf (nb-atomic-queue-first queue) 0)
					(decf  (nb-atomic-queue-size queue))
					0)
				(progn
					(decf (nb-atomic-queue-size queue))
					(incf (nb-atomic-queue-first queue))))))
	
(defun queue-add (queue entry)
	(if (queue-full-p queue)
			nil
			(progn
				(update-queue queue (nb-atomic-queue-last queue) entry)
				(queue-grow queue)
				entry)))

(defun queue-remove (queue)
	(if (queue-empty-p queue)
			nil
			(progn
				(let ((entry (svref (nb-atomic-queue-queue queue) (nb-atomic-queue-first queue))))
					(queue-shrink queue)
					entry))))
	
(defun update-queue (queue idx val)
	(let* ((q (nb-atomic-queue-queue queue))
				 (old (svref q idx)))
			(loop repeat 20 do
					 (if (eq (sb-lockless::compare-and-swap (svref q idx) old val) old)
							 (return val)
							 (sleep 0.005)))))
			
	


