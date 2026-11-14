;;; clutch-test-er.el --- ER view ERT tests for clutch -*- lexical-binding: t; -*-

;; SPDX-License-Identifier: GPL-3.0-or-later

;;; Commentary:

;; Tests for the ER view of a table and the text layout it draws with.

;;; Code:

(eval-and-compile
  (require 'clutch-test-common))

(require 'clutch-er)

(defun clutch-test-er--conn ()
  "Return a test connection declaring orders, its keys and its neighbours."
  (make-clutch-test-conn
   :params '(:host "db" :database "shop")
   :tables
   `(("orders"
      :columns ((:name "id" :type "int" :primary-key t)
                (:name "customer_id" :type "int")
                (:name "region" :type "text")
                (:name "country" :type "text")
                (:name "note" :type "text"))
      :foreign-keys
      (("customer_id" :ref-table "customers" :ref-column "id"
        :constraint "fk_customer")
       ("region" :ref-table "regions" :ref-column "code"
        :constraint "fk_region")
       ("country" :ref-table "regions" :ref-column "country"
        :constraint "fk_region"))
      :referencing-keys
      ((:table "items" :column "order_id" :ref-column "id"
        :constraint "fk_items")
       (:table "refunds" :column "order_id" :ref-column "id"
        :constraint "fk_refunds")))
     ("customers"
      :columns ((:name "id" :type "int" :primary-key t)
                (:name "name" :type "text"))
      :foreign-keys nil
      :referencing-keys
      ((:table "orders" :column "customer_id" :ref-column "id"
        :constraint "fk_customer"))))))

(defmacro clutch-test-er--with-view (conn &rest body)
  "Open the ER view of orders on CONN, run BODY in it, then kill it."
  (declare (indent 1) (debug (form body)))
  (let ((buffer (make-symbol "buffer")))
    `(let ((,buffer nil))
       (unwind-protect
           (progn
             (with-temp-buffer
               (setq-local clutch-connection ,conn)
               (cl-letf (((symbol-function 'pop-to-buffer)
                          (lambda (buf &rest _) (setq ,buffer buf))))
                 (clutch-er-diagram '(:name "orders" :type "TABLE"))))
             (with-current-buffer ,buffer ,@body))
         (when (buffer-live-p ,buffer)
           (kill-buffer ,buffer))))))

(defun clutch-test-er--text ()
  "Return the current buffer's text without properties."
  (buffer-substring-no-properties (point-min) (point-max)))

(defun clutch-test-er--goto (text)
  "Move point to the line containing TEXT."
  (goto-char (point-min))
  (search-forward text)
  (beginning-of-line))

(defun clutch-test-er--highlighted ()
  "Return the highlighted lines, without properties, in buffer order."
  (sort (mapcar (lambda (overlay)
                  (string-trim (buffer-substring-no-properties
                                (overlay-start overlay)
                                (overlay-end overlay))))
                (seq-filter (lambda (overlay)
                              (eq (overlay-get overlay 'face)
                                  'clutch-er-highlight))
                            (overlays-in (point-min) (point-max))))
        #'string<))

(ert-deftest clutch-test-er-shows-key-columns-and-relations ()
  "A key over two columns is one relation, and only key columns show
until all columns are asked for."
  (clutch-test-er--with-view (clutch-test-er--conn)
    (should (equal (clutch-test-er--text)
                   "\
┌ orders ───────────────┐
│ ◆←  id           int  │
│  →  customer_id  int  │
│  →  region       text │
│  →  country      text │
│ … 1 more column       │
└───────────────────────┘

References (2)
  customer_id        →  customers (id)
  (region, country)  →  regions (code, country)

Referenced by (2)
  items (order_id)    →  id
  refunds (order_id)  →  id
"))
    (clutch-er-toggle-columns)
    (should (string-match-p "^│     note         text │$"
                            (clutch-test-er--text)))
    (should-not (string-match-p "more column" (clutch-test-er--text)))))

(ert-deftest clutch-test-er-highlight-follows-point-without-redrawing ()
  "Moving to a relation highlights it and its columns; moving to a column
highlights its relations.  Neither changes the text."
  (clutch-test-er--with-view (clutch-test-er--conn)
    (let ((tick (buffer-chars-modified-tick)))
      (clutch-test-er--goto "regions")
      (run-hooks 'post-command-hook)
      (should (equal (clutch-test-er--highlighted)
                     '("(region, country)  →  regions (code, country)"
                       "│  →  country      text │"
                       "│  →  region       text │")))
      (clutch-test-er--goto "◆←")
      (run-hooks 'post-command-hook)
      (should (equal (clutch-test-er--highlighted)
                     '("items (order_id)    →  id"
                       "refunds (order_id)  →  id"
                       "│ ◆←  id           int  │")))
      (goto-char (point-min))
      (run-hooks 'post-command-hook)
      (should-not (clutch-test-er--highlighted))
      (should (= tick (buffer-chars-modified-tick))))))

(ert-deftest clutch-test-er-follows-relations-and-returns ()
  "Following a relation centers its other table; back returns."
  (clutch-test-er--with-view (clutch-test-er--conn)
    (clutch-test-er--goto "customers (id)")
    (clutch-er-follow)
    (should (string-prefix-p "┌ customers " (clutch-test-er--text)))
    (should (string-match-p "^  orders (customer_id)  →  id$"
                            (clutch-test-er--text)))
    (clutch-er-back)
    (should (string-prefix-p "┌ orders " (clutch-test-er--text)))
    (should-error (clutch-er-back) :type 'user-error)))

(ert-deftest clutch-test-er-refuses-what-it-cannot-show ()
  "A connection that cannot list referencing keys and an object that is
not a table are refused before a view opens."
  (let ((buffers (buffer-list)))
    (with-temp-buffer
      (setq-local clutch-connection
                  (make-clutch-test-conn :table "orders" :columns nil))
      (should-error (clutch-er-diagram '(:name "orders" :type "TABLE"))
                    :type 'user-error)
      (should-error (clutch-er-diagram '(:name "orders_v" :type "VIEW"))
                    :type 'user-error))
    (should-not (cl-set-difference (buffer-list) buffers))))

(ert-deftest clutch-test-er-falls-back-to-ascii ()
  "Where its glyphs are two columns wide, the view draws with ASCII."
  (cl-letf (((symbol-function 'char-width) (lambda (_char) 2)))
    (clutch-test-er--with-view (clutch-test-er--conn)
      (should (string-prefix-p "+ orders -" (clutch-test-er--text)))
      (should (string-match-p "^| \\*<  id  " (clutch-test-er--text)))
      (should (string-match-p "^  customer_id  +->  customers (id)$"
                              (clutch-test-er--text))))))

(provide 'clutch-test-er)

;;; clutch-test-er.el ends here
