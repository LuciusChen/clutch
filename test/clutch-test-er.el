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
        :constraint "fk_customer")))
     ("regions"
      :columns ((:name "code" :type "text" :primary-key t)
                (:name "country" :type "text" :primary-key t))))))

(defmacro clutch-test-er--with-view (conn &rest body)
  "Open the ER view of orders on CONN, run BODY in it, then kill it.
Relations added by hand are kept in a temporary directory."
  (declare (indent 1) (debug (form body)))
  (let ((buffer (make-symbol "buffer"))
        (connection (make-symbol "connection")))
    `(let ((,buffer nil)
           (,connection ,conn)
           (clutch-er-relations-directory (make-temp-file "clutch-er-" t)))
       (unwind-protect
           (progn
             (with-temp-buffer
               (setq-local clutch-connection ,connection
                           clutch--connection-params
                           (clutch-test-conn-params ,connection))
               (cl-letf (((symbol-function 'pop-to-buffer)
                          (lambda (buf &rest _) (setq ,buffer buf))))
                 (clutch-er-diagram '(:name "orders" :type "TABLE"))))
             (with-current-buffer ,buffer ,@body))
         (when (buffer-live-p ,buffer)
           (kill-buffer ,buffer))
         (delete-directory clutch-er-relations-directory t)))))

(defun clutch-test-er--file ()
  "Return the relations file of the test connection."
  (clutch-er--relations-file clutch-connection clutch--connection-params))

(defun clutch-test-er--file-text ()
  "Return the text of the relations file of the test connection."
  (let ((file (clutch-test-er--file)))
    (with-temp-buffer
      (insert-file-contents file)
      (buffer-string))))

(defmacro clutch-test-er--answering (columns table &rest body)
  "Run BODY answering column reads with COLUMNS in turn and the table read
with TABLE.  Return the column lists offered first, in order."
  (declare (indent 2) (debug (form form body)))
  (let ((answers (make-symbol "answers"))
        (offered (make-symbol "offered")))
    `(let ((,answers (copy-sequence ,columns))
           (,offered nil))
       (cl-letf (((symbol-function 'completing-read-multiple)
                  (lambda (_prompt _names _predicate _require initial &rest _)
                    (push initial ,offered)
                    (or (pop ,answers) (error "No column answer left"))))
                 ((symbol-function 'clutch-object-read)
                  (lambda (&rest _) (list :name ,table :type "TABLE"))))
         ,@body)
       (should-not ,answers)
       (nreverse ,offered))))

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
  "Each column shows the relations it ends beside it, a key over two
adjacent columns as one bracket, and only key columns show until all
columns are asked for."
  (clutch-test-er--with-view (clutch-test-er--conn)
    (should (equal (clutch-test-er--text)
                   "
 orders                                    5 columns
 ───────────────────────────────────────────────────
 ◆ id            int     ←   items.order_id
                         ←   refunds.order_id
 ● customer_id   int     →   customers.id
 ● region        text   ─┐
 ● country       text   ─┴→  regions (code, country)
   … 1 more column
"))
    (clutch-er-toggle-columns)
    (should (string-match-p "^   note +text$" (clutch-test-er--text)))
    (should-not (string-match-p "more column" (clutch-test-er--text)))))

(ert-deftest clutch-test-er-highlight-follows-point-without-redrawing ()
  "Moving to a relation highlights it and its columns; moving to a column
highlights its relations.  Neither changes the text."
  (clutch-test-er--with-view (clutch-test-er--conn)
    (let ((tick (buffer-chars-modified-tick)))
      (clutch-test-er--goto "regions")
      (run-hooks 'post-command-hook)
      (should (equal (clutch-test-er--highlighted)
                     '("● country       text   ─┴→  regions (code, country)" "● region        text   ─┐")))
      (clutch-test-er--goto "◆")
      (run-hooks 'post-command-hook)
      (should (equal (clutch-test-er--highlighted)
                     '("←   refunds.order_id" "◆ id            int     ←   items.order_id")))
      (goto-char (point-min))
      (run-hooks 'post-command-hook)
      (should-not (clutch-test-er--highlighted))
      (should (= tick (buffer-chars-modified-tick))))))

(ert-deftest clutch-test-er-follows-relations-and-returns ()
  "Following a relation centers its other table; back returns."
  (clutch-test-er--with-view (clutch-test-er--conn)
    (clutch-test-er--goto "customers.id")
    (clutch-er-follow)
    (should (string-match-p "\\`\n customers " (clutch-test-er--text)))
    (should (string-match-p "←   orders.customer_id$" (clutch-test-er--text)))
    (clutch-er-back)
    (should (string-match-p "\\`\n orders " (clutch-test-er--text)))
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

(ert-deftest clutch-test-er-relations-by-hand-are-added-changed-and-deleted ()
  "A relation added by hand is stored in its file and shown with a dotted
link; it can change and go, and it cannot repeat a stored relation or a
key of the database."
  (clutch-test-er--with-view (clutch-test-er--conn)
    (dolist (columns '(("note") ("id" "note")))
      (should (equal (clutch-test-er--answering (list columns columns) "orders"
                       (clutch-er--read-relation clutch-connection
                                                 clutch-er--entry columns))
                     (list (string-join columns ",")
                           (string-join columns ",")))))
    (clutch-er-toggle-columns)
    (clutch-test-er--goto "note")
    (should (equal (clutch-test-er--answering '(("note") ("name")) "customers"
                     (clutch-er-add-relation))
                   '("note" nil)))
    (should (equal (clutch-test-er--file-text)
                   "\
{
  \"version\": 1,
  \"relations\": [
    {
      \"from\": {\"table\":\"orders\"},
      \"to\": {\"table\":\"customers\"},
      \"columns\": [[\"note\",\"name\"]]
    }
  ]
}
"))
    (should (string-match-p "^ ○ note +text +⇢   customers.name  manual$"
                            (clutch-test-er--text)))
    (clutch-test-er--goto "customers.name")
    (should (equal (clutch-test-er--answering '(("note") ("id")) "customers"
                     (clutch-er-edit-relation))
                   '("note" "name")))
    (should (string-match-p (regexp-quote "[[\"note\",\"id\"]]")
                            (clutch-test-er--file-text)))
    (should-error (clutch-test-er--answering '(("note") ("id")) "customers"
                    (clutch-er-add-relation))
                  :type 'user-error)
    (should-error (clutch-test-er--answering '(("region" "country")
                                               ("code" "country"))
                      "regions"
                    (clutch-er-add-relation))
                  :type 'user-error)
    (clutch-test-er--goto "customers.id")
    (should-error (clutch-er-delete-relation) :type 'user-error)
    (clutch-test-er--goto "customers.id  manual")
    (cl-letf (((symbol-function 'y-or-n-p) (lambda (_prompt) t)))
      (clutch-er-delete-relation))
    (should (equal (clutch-test-er--file-text)
                   "{\n  \"version\": 1,\n  \"relations\": []\n}\n"))
    (should-not (string-match-p "manual" (clutch-test-er--text)))))

(ert-deftest clutch-test-er-relations-by-hand-show-at-both-ends ()
  "A stored relation shows from either table, and one that repeats a key
of the database shows only as that key.  A missing schema means the
connection's current one."
  (clutch-test-er--with-view (clutch-test-er--conn)
    ;; Database-only navigation must not resolve the namespace over the wire.
    (cl-letf (((symbol-function 'clutch-db-current-schema)
               (lambda (&rest _) (ert-fail "Unexpected namespace lookup"))))
      (dolist (empty-file '(nil t))
        (ert-info ((if empty-file "empty relations file" "no relations file"))
          (when empty-file
            (clutch-er--write-relations (clutch-test-er--file) nil))
          (should (= 2 (length (clutch-er-outgoing-relations
                                clutch-connection clutch--connection-params
                                clutch-er--entry)))))))
    (make-directory clutch-er-relations-directory t)
    (with-temp-file (clutch-test-er--file)
      (insert "{\"version\": 1, \"relations\": [
 {\"from\": {\"table\": \"orders\"}, \"to\": {\"table\": \"customers\"},
  \"columns\": [[\"customer_id\", \"id\"]]},
 {\"from\": {\"table\": \"orders\"}, \"to\": {\"table\": \"customers\"},
  \"columns\": [[\"note\", \"name\"]]}]}"))
    (should (= 3 (length (clutch-er-outgoing-relations
                          clutch-connection clutch--connection-params
                          clutch-er--entry))))
    (clutch-er-refresh)
    (should (string-match-p "^ ● customer_id +int +→   customers.id$"
                            (clutch-test-er--text)))
    (should (= 1 (cl-count-if (lambda (line) (string-match-p "customers.id" line))
                              (split-string (clutch-test-er--text) "\n"))))
    (clutch-test-er--goto "customers.name")
    (clutch-er-follow)
    (should (string-match-p "←   orders.customer_id$" (clutch-test-er--text)))
    (should (string-match-p "⇠   orders.note  manual$" (clutch-test-er--text)))
    (clutch-er-back)
    (clutch-er--write-relations
     (clutch-test-er--file)
     '(((from . ((table . "orders")))
        (to . ((catalog . "archive") (schema . "public") (table . "customers")))
        (columns . [["note" "name"]]))))
    (push (list (clutch--table-key "customers" "public" "archive")
                :columns '((:name "name" :type "text")))
          (clutch-test-conn-tables clutch-connection))
    (clutch-er-refresh)
    (clutch-test-er--goto "archive.public.customers.name")
    (clutch-er-follow)
    (should (equal (plist-get clutch-er--entry :catalog) "archive"))))

(ert-deftest clutch-test-er-refuses-a-relations-file-it-cannot-read ()
  "A file that is not JSON, or is of another version, is reported by name
and leaves the view as it was."
  (clutch-test-er--with-view (clutch-test-er--conn)
    (let ((text (clutch-test-er--text)))
      (make-directory clutch-er-relations-directory t)
      (dolist (content '("{\"version\": 2, \"relations\": []}"
                         "{\"version\": 1, \"relations\": [{\"from\": 1}]}"
                         "not json"))
        (with-temp-file (clutch-test-er--file) (insert content))
        (let ((err (should-error (clutch-er-refresh) :type 'user-error)))
          (should (string-match-p (regexp-quote (clutch-test-er--file))
                                  (cadr err))))
        (should (equal text (clutch-test-er--text)))))))

(ert-deftest clutch-test-er-relations-files-distinguish-targets ()
  "Relations are isolated by target and transport, with no URL secret in a name."
  (let ((conn (clutch-test-er--conn)))
    (dolist (pair '(((:backend sqlite :database "/tmp/review_a.db")
                     (:backend sqlite :database "/tmp/review/a.db"))
                    ((:backend pg :host "127.0.0.1" :port 5432 :database "app" :ssh-host "a")
                     (:backend pg :host "127.0.0.1" :port 5432 :database "app" :ssh-host "b"))))
      (should-not (equal (clutch-er--relations-file conn (car pair))
                         (clutch-er--relations-file conn (cadr pair)))))
    (should (equal (clutch-er--relations-file
                    conn '(:backend pg :database "shop" :schema "public"))
                   (clutch-er--relations-file
                    conn '(:backend pg :database "shop" :schema "other"))))
    (let* ((params '(:backend jdbc :url "jdbc:postgresql://db/app?password=REVIEW_SECRET"))
           (file (clutch-er--relations-file conn params)))
      (should-not (string-match-p "REVIEW_SECRET" file))
      (should (equal file (clutch-er--relations-file
                           conn '(:backend jdbc :url "jdbc:postgresql://db/app?password=CHANGED_SECRET")))))))

(ert-deftest clutch-test-er-keeps-relations-with-missing-columns ()
  "A removed column leaves its relation visible and deletable."
  (clutch-test-er--with-view (clutch-test-er--conn)
    (clutch-test-er--answering '(("id" "note") ("id" "name")) "customers"
      (clutch-er-add-relation))
    (setf (clutch-test-conn-tables clutch-connection)
          (copy-tree (clutch-test-conn-tables clutch-connection)))
    (let* ((table (assoc "orders" (clutch-test-conn-tables clutch-connection)))
           (columns (plist-get (cdr table) :columns)))
      (setcdr table (plist-put (cdr table) :columns
                               (cl-remove "note" columns :test #'equal
                                          :key (lambda (column) (plist-get column :name))))))
    (clutch-er-refresh)
    (should (string-match-p "note +(missing column)" (clutch-test-er--text)))
    (clutch-test-er--goto "manual")
    (cl-letf (((symbol-function 'y-or-n-p) (lambda (_) t)))
      (clutch-er-delete-relation))
    (should-not (string-match-p "missing column" (clutch-test-er--text)))))

(ert-deftest clutch-test-er-relations-file-keeps-names-as-written ()
  "Names keep their case and characters, written as UTF-8, and a file
that is written and read back holds the same relations."
  (let* ((directory (make-temp-file "clutch-er-" t))
         (file (expand-file-name "r.json" directory))
         (relations '(((from . ((schema . "Sales") (table . "订单")))
                       (to . ((table . "Customer")))
                       (columns . [["客户号" "ID"]])))))
    (unwind-protect
        (progn
          (clutch-er--write-relations file relations)
          (should (equal (clutch-er--read-relations file) relations))
          (should (string-match-p
                   (regexp-quote "{\"schema\":\"Sales\",\"table\":\"订单\"}")
                   (with-temp-buffer
                     (let ((coding-system-for-read 'utf-8))
                       (insert-file-contents file))
                     (buffer-string)))))
      (delete-directory directory t))))

(ert-deftest clutch-test-er-result-column-starts-a-relation ()
  "A relation added from a result starts at the source column at point,
not the name the result shows it under."
  (let* ((conn (clutch-test-er--conn))
         (clutch-er-relations-directory (make-temp-file "clutch-er-" t)))
    (unwind-protect
        (clutch-test--with-result-state
            (:connection conn
             :connection-params (clutch-test-conn-params conn)
             :source-table "orders"
             :columns '("id" "label")
             :column-defs '((:name "id" :source-column "id")
                            (:name "label" :source-column "note"))
             :rows '((1 "x"))
             :render t)
          (goto-char (point-min))
          (search-forward "x")
          (should (equal (clutch-test-er--answering '(("note") ("name"))
                             "customers"
                           (clutch-result-add-relation))
                         '("note" nil)))
          (should (string-match-p (regexp-quote "[[\"note\",\"name\"]]")
                                  (clutch-test-er--file-text))))
      (delete-directory clutch-er-relations-directory t))))

(ert-deftest clutch-test-er-falls-back-to-ascii ()
  "Where its symbols are two columns wide, or when asked to, the view
draws with ASCII."
  (cl-letf (((symbol-function 'char-width) (lambda (_char) 2)))
    (clutch-test-er--with-view (clutch-test-er--conn)
      (should (string-match-p "^ \\* id +int +<-- items.order_id$"
                              (clutch-test-er--text)))
      (should (string-match-p "^ o country +text +-\\+> regions (code, country)$"
                              (clutch-test-er--text)))))
  (let ((clutch-er-charset 'ascii))
    (clutch-test-er--with-view (clutch-test-er--conn)
      (should (string-match-p "--> customers.id$" (clutch-test-er--text))))))

(ert-deftest clutch-test-er-async-load-owns-its-reply ()
  "ER loads defer rendering and release their activity on every outcome."
  (dolist (case '(normal cancelled killed moved render-error dispatch-error))
    (ert-info ((symbol-name case))
      (clutch-test-er--with-view (clutch-test-er--conn)
        (let ((conn clutch-connection)
              (entry clutch-er--entry)
              reply active)
          (cl-letf (((symbol-function 'clutch-db-referencing-keys-async)
                     (lambda (_conn _table callback &optional _errback _schema _catalog continue)
                       (when (eq case 'dispatch-error) (error "Dispatch failed"))
                       (setq reply callback active continue)
                       t)))
            (if (eq case 'dispatch-error)
                (should-error (clutch-er-diagram entry))
              (clutch-er-diagram entry)
              (should reply)
              (should (string-match-p "Loading relations" (buffer-string)))
              (should (gethash conn clutch--running-queries))
              (pcase case
                ('cancelled
                 ;; The backend declines cancellation; later workflow steps
                 ;; and presentation must still stop on the user's request.
                 (cl-letf (((symbol-function 'clutch-db-interrupt-query)
                            (lambda (_) nil)))
                   (clutch-cancel-query-or-quit)))
                ('moved (setq-local clutch-connection (clutch-test-er--conn)))
                ('killed (kill-buffer (current-buffer))))
              (should (eq (and (funcall active) t)
                          (and (memq case '(normal render-error)) t)))
              (let (finish)
                (cl-letf (((symbol-function 'run-with-idle-timer)
                           (lambda (_delay _repeat function &rest args)
                             (setq finish (lambda () (apply function args))))))
                  (funcall reply nil))
                (if (eq case 'render-error)
                    (let ((clutch--table-metadata-updated-hook
                           (list (lambda (&rest _) (error "Render failed")))))
                      (should-error (funcall finish))
                      (should (string-match-p "Render failed" (buffer-string))))
                  (funcall finish)))
              (pcase case
                ('normal (should clutch-er--model))
                ('cancelled (should (string-match-p "cancelled" (buffer-string))))))
          (should-not (gethash conn clutch--running-queries))
          (should-not (clutch-db-in-use-p conn))))))))

(provide 'clutch-test-er)

;;; clutch-test-er.el ends here
