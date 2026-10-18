;;; clutch-searchable-header-gui.el --- Native header search probe -*- lexical-binding: t; -*-

;; SPDX-License-Identifier: GPL-3.0-or-later

;;; Commentary:
;; Run an isolated graphical Emacs with -Q -l /absolute/path/to/this-file.
;; CLUTCH_SEARCH_CHECKOUT selects the checkout; CLUTCH_SEARCH_OUTPUT is JSON.
;; CLUTCH_SEARCH_EXTRA_LOAD_PATH can supply Transient and its dependencies.

;;; Code:

(setq inhibit-startup-screen t initial-scratch-message nil
      native-comp-jit-compilation nil debug-on-error nil)
(let ((root (file-name-as-directory (getenv "CLUTCH_SEARCH_CHECKOUT"))))
  (add-to-list 'load-path root)
  (add-to-list 'load-path (concat root "test")))
(dolist (path (split-string (or (getenv "CLUTCH_SEARCH_EXTRA_LOAD_PATH") "")
                           path-separator t))
  (add-to-list 'load-path path))
(require 'clutch-test-common)
(require 'json)
(tool-bar-mode -1)
(set-frame-name "Clutch searchable header isolated test")
(set-frame-size (selected-frame) 95 36)
(set-face-attribute 'default nil :family "Menlo" :height 130)

(defun clutch-search-gui--settle ()
  "Finish pending GUI redisplay."
  (redisplay t)
  (sit-for 0.12)
  (redisplay t))

(defvar clutch-search-gui--phase "start")

(defun clutch-search-gui--borders (y)
  "Return visible table border coordinates at window pixel Y."
  (let (previous borders)
    (dotimes (x (window-body-width nil t))
      (let* ((position (posn-at-x-y x y (selected-window)))
             (index (posn-point position)))
        (when (and (integerp index) (eq (char-after index) ?│)
                   (not (equal index previous)))
          (push x borders))
        (setq previous index)))
    (nreverse borders)))

(defun clutch-search-gui--run ()
  "Exercise real isearch commands, including RET, C-g and horizontal scrolling."
  (condition-case error
      (let* ((buffer (get-buffer-create "*clutch searchable header*"))
             (columns (append '("id" "customer_email")
                              (cl-loop for i from 2 to 11 collect (format "column_%02d" i))
                              '("long_customer_shipping_address" "中文列名")))
             (definitions (mapcar (lambda (name) (list :name name)) columns))
             (rows (cl-loop for row below 100
                            collect (cons row (cl-loop repeat 13 collect (format "value_%03d" row)))))
             (widths (make-vector 14 12))
             (checks nil))
        (aset widths 1 20)
        (aset widths 12 9)
        (setq clutch-search-gui--phase "fixture")
        (switch-to-buffer buffer)
        (let ((inhibit-read-only t))
          (clutch-test--init-result-state
           (list :columns columns :column-defs definitions :rows rows
                 :column-widths widths :connection nil :render t)))
        (setq clutch-search-gui--phase "enable")
        (clutch-result-searchable-header-mode 1)
        (clutch--goto-cell 80 0)
        (set-window-start nil (aref clutch--row-start-positions 70))
        (clutch-search-gui--settle)
        (dolist (spec '(("customer_email" 1) ("long_customer_shipping_address" 12)
                        ("中文列名" 13)))
          (let ((name (car spec)) (column (cadr spec)))
            (setq clutch-search-gui--phase (concat "search " name))
            (execute-kbd-macro (vconcat (kbd "C-r") name (kbd "RET")))
            (clutch-search-gui--settle)
            (cl-assert (= (clutch--searchable-header-column) column))
            (cl-assert (pos-visible-in-window-p (point)))
            (cl-assert (eq (window-parameter nil 'header-line-format) 'none))
            (cl-assert (null (get-text-property (point) 'clutch-row-idx)))
            (push (list :name name :column column :point (point)
                        :visible t :hscroll (window-hscroll)) checks)
            (setq clutch-search-gui--phase "data navigation")
            (execute-kbd-macro (kbd "n"))
            (clutch-search-gui--settle)
            (cl-assert (= (get-text-property (point) 'clutch-row-idx) 0))
            (cl-assert (= (get-text-property (point) 'clutch-col-idx) column))
            (cl-assert (not (window-parameter nil 'header-line-format)))))
        ;; A forward search starting in data wraps back to the actual header.
        (setq clutch-search-gui--phase "forward wrap")
        (clutch--goto-cell 20 0)
        ;; A failed search rings the bell and stops keyboard macros, so drive
        ;; this native isearch session directly through its public commands.
        (isearch-forward nil t)
        (isearch-yank-string "customer_email")
        (cl-assert (not isearch-success))
        (isearch-repeat-forward)
        (cl-assert isearch-success)
        (clutch-search-gui--settle)
        (cl-assert (pos-visible-in-window-p (point)))
        (cl-assert (eq (window-parameter nil 'header-line-format) 'none))
        (isearch-exit)
        (clutch-search-gui--settle)
        (cl-assert (= (clutch--searchable-header-column) 1))
        (push (list :name "forward-wrap" :point (point) :visible t :while-searching t) checks)
        ;; Aborting an active search returns to the exact original data point.
        (setq clutch-search-gui--phase "abort")
        (clutch--goto-cell 65 3)
        (clutch-search-gui--settle)
        (let ((position (point)))
          (condition-case nil
              (execute-kbd-macro (vconcat (kbd "C-r") "customer_email" (kbd "C-g")))
            (quit nil))
          (clutch-search-gui--settle)
          (cl-assert (= (point) position))
          (cl-assert (not (window-parameter nil 'header-line-format))))
        (push (list :name "abort-restores-data-point" :passed t) checks)
        (setq clutch-search-gui--phase "real header alignment")
        (let ((auto-hscroll-mode nil) cases)
          (dolist (scale '(0 2))
            (text-scale-set scale)
            (goto-char (point-min))
            (set-window-start nil (point-min))
            (dolist (offset '(0 15 100))
              (set-window-hscroll nil offset)
              (clutch-search-gui--settle)
              (let* ((height (car (window-line-height 0)))
                     (header (clutch-search-gui--borders (/ height 2)))
                     (data (clutch-search-gui--borders (+ height (/ height 2)))))
                (cl-assert header nil "No borders: scale=%s hscroll=%s" scale offset)
                (cl-assert (equal header data) nil
                           "scale=%s hscroll=%s height=%s header=%S data=%S"
                           scale offset height header data)
                (push (list :scale scale :hscroll offset :borders (vconcat header)) cases))))
          (text-scale-set 0)
          (push (list :name "real-header-aligns-with-data" :cases (vconcat cases)) checks))
        ;; The real header is not a data row, including for copies and edits.
        (cl-assert (= (length (clutch--rows-in-region (point-min) (point-max))) 100))
        (cl-assert (= (length (window-list)) 1))
        (cl-assert (not (featurep 'etaf)))
        (cl-assert (not (featurep 'clutch-browser)))
        (with-temp-file (getenv "CLUTCH_SEARCH_OUTPUT")
          (insert (json-encode (list :emacs emacs-version :status "pass"
                                    :source (symbol-file 'clutch--render-result)
                                    :checks (vconcat (nreverse checks)))) "\n"))
        (if (getenv "CLUTCH_SEARCH_HOLD")
            (progn
              (clutch--goto-cell 80 0)
              (set-window-hscroll nil 0)
              (execute-kbd-macro (vconcat (kbd "C-r") "customer_email" (kbd "RET")))
              (clutch-search-gui--settle))
          (kill-emacs 0)))
    (error
     (with-temp-file (getenv "CLUTCH_SEARCH_OUTPUT")
       (insert (json-encode (list :emacs emacs-version :status "fail"
                                 :phase clutch-search-gui--phase :error (error-message-string error)
                                 :message (current-message)))))
     (kill-emacs 1))))

(run-with-timer 0.2 nil #'clutch-search-gui--run)

;;; clutch-searchable-header-gui.el ends here
