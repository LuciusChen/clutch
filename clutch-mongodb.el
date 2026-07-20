;;; clutch-mongodb.el --- Native MongoDB backend adapter -*- lexical-binding: t; -*-

;; Copyright (C) 2025-2026 Lucius Chen
;; SPDX-License-Identifier: GPL-3.0-or-later


;; This file is part of clutch.

;; clutch is free software: you can redistribute it and/or modify
;; it under the terms of the GNU General Public License as published by
;; the Free Software Foundation, either version 3 of the License, or
;; (at your option) any later version.

;; clutch is distributed in the hope that it will be useful,
;; but WITHOUT ANY WARRANTY; without even the implied warranty of
;; MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;; GNU General Public License for more details.

;; You should have received a copy of the GNU General Public License
;; along with clutch.  If not, see <https://www.gnu.org/licenses/>.

;;; Commentary:

;; Native MongoDB backend for the clutch generic database interface.
;;
;; This backend intentionally targets ordinary MongoDB connections through the
;; public mongodb.el client API.  It is separate from MongoDB SQL Interface, which
;; is an optional JDBC surface.
;;
;; Query text supports a practical subset of MongoDB Shell helper calls.  The
;; backend translates those helpers into public mongodb.el command calls.  Cursor
;; results are converted to arrays, documents are flattened by top-level keys,
;; and nested documents/arrays are rendered as JSON-valued cells.

;;; Code:

(require 'cl-lib)
(require 'clutch-backend)
(require 'json)
(require 'seq)
(require 'subr-x)

(declare-function clutch-db-jdbc-connect "clutch-db-jdbc" (driver params))
(declare-function mongodb-aggregate "mongodb" (client database collection pipeline &optional options))
(declare-function mongodb-aggregate-command "mongodb" (collection pipeline &optional options))
(declare-function mongodb-command "mongodb" (client database command &optional timeout sequences))
(declare-function mongodb-connect "mongodb" (params))
(declare-function mongodb-count-documents "mongodb" (client database collection filter &optional options))
(declare-function mongodb-delete "mongodb" (client database collection filter &optional multi))
(declare-function mongodb-datetime "mongodb" (millis))
(declare-function mongodb-disconnect "mongodb" (conn))
(declare-function mongodb-distinct "mongodb" (client database collection field-name &optional query options))
(declare-function mongodb-document "mongodb" (elements))
(declare-function mongodb-document-elements "mongodb" (document))
(declare-function mongodb-document-p "mongodb" (value))
(declare-function mongodb-error-labels "mongodb" (condition))
(declare-function mongodb-explain "mongodb" (client database command &optional verbosity))
(declare-function mongodb-find "mongodb" (client database collection filter &optional projection limit skip sort options))
(declare-function mongodb-find-command "mongodb" (collection filter &optional projection limit skip sort options))
(declare-function mongodb-insert "mongodb" (client database collection documents))
(declare-function mongodb-connection-host "mongodb" (conn))
(declare-function mongodb-connection-port "mongodb" (conn))
(declare-function mongodb-connection-username "mongodb" (conn))
(declare-function mongodb-list-collection-docs "mongodb" (client database &optional filter options))
(declare-function mongodb-list-collections "mongodb" (client database &optional filter options))
(declare-function mongodb-list-indexes "mongodb" (client database collection))
(declare-function mongodb-live-p "mongodb" (conn))
(declare-function mongodb-object-id "mongodb" (hex))
(declare-function mongodb-update "mongodb" (client database collection filter update &optional multi upsert))
(declare-function mongodb-conn-database "mongodb" (conn))

;;;; Configuration

(defcustom clutch-mongodb-schema-sample-size 20
  "Number of MongoDB documents to sample for collection field metadata."
  :type 'integer
  :group 'clutch)

(defcustom clutch-mongodb-find-result-limit 1000
  "Maximum documents returned by a native MongoDB `find' helper.
An explicit `.limit(N)' must also fit this bound.  This keeps the synchronous
Emacs result contract from materializing an unbounded collection."
  :type 'integer
  :group 'clutch)

(defconst clutch-mongodb--source-document-column "clutch__document"
  "Hidden result column name that stores the original MongoDB document.")

(defun clutch-mongodb--error-message (err)
  "Return the primary MongoDB error message from ERR."
  (if (stringp (cadr err))
      (cadr err)
    (error-message-string err)))

(defun clutch-mongodb--error-details (err)
  "Return Clutch details plist for MongoDB ERR."
  (when-let* ((labels (mongodb-error-labels err)))
    (list :mongodb-error-labels labels
          :diag (list :mongodb-error-labels labels))))

(defmacro clutch-mongodb--with-mongodb-errors (&rest body)
  "Run BODY and translate `mongodb-error' to `clutch-db-error'."
  (declare (indent 0) (debug t))
  `(condition-case err
       (progn ,@body)
     (mongodb-error
      (let ((message (clutch-mongodb--error-message err))
            (details (clutch-mongodb--error-details err)))
        (signal 'clutch-db-error
                (if details
                    (list message details)
                  (list message)))))))

;;;; Connection struct

(cl-defstruct clutch-mongodb-conn
  "A logical MongoDB connection executed through mongodb.el."
  params
  database
  client
  closed
  busy)

;;;; Connect function

(defun clutch-mongodb--surface (params)
  "Return normalized MongoDB surface from PARAMS, or nil."
  (clutch-db--normalize-symbol-option (plist-get params :surface)))

(defun clutch-mongodb--validate-surface (params)
  "Signal if PARAMS contain an unsupported MongoDB surface."
  (when-let* ((surface (clutch-mongodb--surface params)))
    (unless (clutch-backend-surface-feature 'mongodb surface)
      (signal 'clutch-db-error
              (list (format "Unsupported MongoDB :surface %S" surface))))))

(defconst clutch-mongodb--required-mongodb-functions
  '(mongodb-aggregate
    mongodb-aggregate-command
    mongodb-command
    mongodb-connect
    mongodb-count-documents
    mongodb-delete
    mongodb-datetime
    mongodb-disconnect
    mongodb-distinct
    mongodb-document
    mongodb-document-elements
    mongodb-document-p
    mongodb-error-labels
    mongodb-explain
    mongodb-find
    mongodb-find-command
    mongodb-insert
    mongodb-list-collection-docs
    mongodb-list-collections
    mongodb-list-indexes
    mongodb-live-p
    mongodb-object-id
    mongodb-update
    mongodb-connection-host
    mongodb-connection-port
    mongodb-connection-username
    mongodb-conn-database)
  "Public mongodb.el functions required by the native MongoDB adapter.")

(defun clutch-mongodb--missing-mongodb-functions ()
  "Return mongodb.el functions required by Clutch but currently unavailable."
  (seq-remove #'fboundp clutch-mongodb--required-mongodb-functions))

(defun clutch-mongodb--ensure-mongodb-client-api ()
  "Load mongodb.el and verify that the native client API is available."
  (unless (featurep 'mongodb)
    (condition-case err
        (require 'mongodb)
      (error
       (signal 'clutch-db-error
               (list (format "MongoDB backend requires mongodb.el: %s"
                             (error-message-string err)))))))
  (when-let* ((missing (clutch-mongodb--missing-mongodb-functions)))
    (signal 'clutch-db-error
            (list (format
                   (concat "MongoDB backend requires current mongodb.el public API; "
                           "missing %s. Loaded library: %s. Update/install "
                           "LuciusChen/mongodb.el, clear stale native-compile cache, "
                           "and restart Emacs.")
                   (mapconcat #'symbol-name missing ", ")
                   (or (locate-library "mongodb") "not found"))))))

(defun clutch-mongodb-connect (params)
  "Connect to MongoDB using PARAMS.
PARAMS may contain :url, or structured :host/:port/:database fields.
The default connection delegates to public mongodb.el APIs.  When PARAMS select
`:surface sql-interface', delegate to the JDBC SQL Interface surface."
  (clutch-mongodb--validate-surface params)
  (if (clutch-backend-jdbc-transport-p 'mongodb params)
      (progn
        (require 'clutch-db-jdbc)
        (clutch-db-jdbc-connect 'mongodb params))
    (clutch-mongodb--ensure-mongodb-client-api)
    (clutch-mongodb--with-mongodb-errors
      (let ((client (mongodb-connect params)))
        (make-clutch-mongodb-conn
         :params (copy-sequence params)
         :database (mongodb-conn-database client)
         :client client
         :closed nil
         :busy nil)))))

;;;; MQL helper parsing

(cl-defstruct clutch-mongodb--mql-reader
  text
  (pos 0))

(defun clutch-mongodb--mql-end-p (reader)
  "Return non-nil when READER is at end of input."
  (>= (clutch-mongodb--mql-reader-pos reader)
      (length (clutch-mongodb--mql-reader-text reader))))

(defun clutch-mongodb--mql-peek (reader)
  "Return current character in READER, or nil."
  (unless (clutch-mongodb--mql-end-p reader)
    (aref (clutch-mongodb--mql-reader-text reader)
          (clutch-mongodb--mql-reader-pos reader))))

(defun clutch-mongodb--mql-read (reader)
  "Read one character from READER."
  (prog1 (clutch-mongodb--mql-peek reader)
    (cl-incf (clutch-mongodb--mql-reader-pos reader))))

(defun clutch-mongodb--mql-skip-space (reader)
  "Skip whitespace in READER."
  (while (and (not (clutch-mongodb--mql-end-p reader))
              (memq (clutch-mongodb--mql-peek reader)
                    '(?\s ?\t ?\n ?\r)))
    (clutch-mongodb--mql-read reader)))

(defun clutch-mongodb--mql-delimiter-p (char)
  "Return non-nil when CHAR terminates an MQL token."
  (or (null char)
      (memq char '(?\s ?\t ?\n ?\r ?{ ?} ?\[ ?\] ?\( ?\) ?: ?, ?\;))))

(defun clutch-mongodb--mql-parse-string (reader)
  "Parse a quoted string from READER."
  (let ((quote (clutch-mongodb--mql-read reader))
        chars)
    (while (and (not (clutch-mongodb--mql-end-p reader))
                (not (eq (clutch-mongodb--mql-peek reader) quote)))
      (let ((char (clutch-mongodb--mql-read reader)))
        (if (eq char ?\\)
            (push (pcase (clutch-mongodb--mql-read reader)
                    (?n ?\n)
                    (?r ?\r)
                    (?t ?\t)
                    (escaped escaped))
                  chars)
          (push char chars))))
    (unless (eq (clutch-mongodb--mql-read reader) quote)
      (signal 'clutch-db-error
              (list "Unterminated MongoDB string literal")))
    (apply #'string (nreverse chars))))

(defun clutch-mongodb--mql-parse-identifier (reader)
  "Parse an identifier token from READER."
  (let ((start (clutch-mongodb--mql-reader-pos reader)))
    (while (not (clutch-mongodb--mql-delimiter-p
                 (clutch-mongodb--mql-peek reader)))
      (clutch-mongodb--mql-read reader))
    (substring (clutch-mongodb--mql-reader-text reader)
               start
               (clutch-mongodb--mql-reader-pos reader))))

(defun clutch-mongodb--mql-parse-number (reader)
  "Parse a numeric literal from READER."
  (let ((start (clutch-mongodb--mql-reader-pos reader)))
    (while (and (not (clutch-mongodb--mql-end-p reader))
                (string-match-p
                 "[0-9eE+.-]"
                 (char-to-string (clutch-mongodb--mql-peek reader))))
      (clutch-mongodb--mql-read reader))
    (let ((token (substring (clutch-mongodb--mql-reader-text reader)
                            start
                            (clutch-mongodb--mql-reader-pos reader))))
      (string-to-number token))))

(defun clutch-mongodb--mql-nonnegative-integer-arg (value method)
  "Return VALUE as a non-negative integer argument for MongoDB METHOD."
  (unless (and (integerp value)
               (<= 0 value))
    (signal 'clutch-db-error
            (list (format "%s() expects one non-negative integer argument"
                          method))))
  value)

(defun clutch-mongodb--mql-boolean-option-arg (args method)
  "Return ARGS parsed as an optional boolean argument for METHOD."
  (cond
   ((null args) t)
   ((and (= (length args) 1)
         (memq (car args) '(t :false)))
    (car args))
   (t
    (signal 'clutch-db-error
            (list (format "%s() expects no arguments or one boolean argument"
                          method))))))

(defun clutch-mongodb--mql-iso-date-millis (string)
  "Return UTC milliseconds for ISODate STRING."
  (let* ((fraction
          (if (string-match
               "\\.[0-9]+\\(?:Z\\|[+-][0-9][0-9]:?[0-9][0-9]\\)?\\'"
               string)
              (substring string (1+ (match-beginning 0))
                         (match-end 0))
            ""))
         (fraction-digits
          (if (string-match "\\`\\([0-9]+\\)" fraction)
              (match-string 1 fraction)
            ""))
         (millis
          (string-to-number
           (substring (concat fraction-digits "000") 0 3)))
         (parsed (parse-time-string string)))
    (setf (nth 0 parsed) (or (nth 0 parsed) 0))
    (setf (nth 1 parsed) (or (nth 1 parsed) 0))
    (setf (nth 2 parsed) (or (nth 2 parsed) 0))
    (setf (nth 8 parsed) (or (nth 8 parsed) 0))
    (unless (and (nth 3 parsed)
                 (nth 4 parsed)
                 (nth 5 parsed))
      (signal 'clutch-db-error
              (list (format "Invalid ISODate() value: %S" string))))
    (+ (floor (* 1000 (float-time (apply #'encode-time parsed))))
       millis)))

(defun clutch-mongodb--mql-expect (reader char)
  "Read CHAR from READER or signal."
  (clutch-mongodb--mql-skip-space reader)
  (unless (eq (clutch-mongodb--mql-read reader) char)
    (signal 'clutch-db-error
            (list (format "Expected `%c' in MongoDB expression" char)))))

(defun clutch-mongodb--mql-parse-object (reader)
  "Parse a MongoDB document literal from READER."
  (let (pairs done)
    (clutch-mongodb--mql-expect reader ?{)
    (while (not done)
      (clutch-mongodb--mql-skip-space reader)
      (if (eq (clutch-mongodb--mql-peek reader) ?})
          (progn
            (clutch-mongodb--mql-read reader)
            (setq done t))
        (let ((key (if (memq (clutch-mongodb--mql-peek reader) '(?\" ?\'))
                       (clutch-mongodb--mql-parse-string reader)
                     (clutch-mongodb--mql-parse-identifier reader))))
          (clutch-mongodb--mql-expect reader ?:)
          (push (cons key (clutch-mongodb--mql-parse-value reader))
                pairs)
          (clutch-mongodb--mql-skip-space reader)
          (pcase (clutch-mongodb--mql-peek reader)
            (?, (clutch-mongodb--mql-read reader))
            (?} nil)
            (_
             (signal 'clutch-db-error
                     (list "Expected `,' or `}' in MongoDB document")))))))
    (mongodb-document (nreverse pairs))))

(defun clutch-mongodb--mql-parse-array (reader)
  "Parse a MongoDB array literal from READER."
  (let (values done)
    (clutch-mongodb--mql-expect reader ?\[)
    (while (not done)
      (clutch-mongodb--mql-skip-space reader)
      (if (eq (clutch-mongodb--mql-peek reader) ?\])
          (progn
            (clutch-mongodb--mql-read reader)
            (setq done t))
        (push (clutch-mongodb--mql-parse-value reader) values)
        (clutch-mongodb--mql-skip-space reader)
        (pcase (clutch-mongodb--mql-peek reader)
          (?, (clutch-mongodb--mql-read reader))
          (?\] nil)
          (_
           (signal 'clutch-db-error
                   (list "Expected `,' or `]' in MongoDB array"))))))
    (vconcat (nreverse values))))

(defun clutch-mongodb--mql-parse-constructor (name reader)
  "Parse constructor NAME from READER."
  (clutch-mongodb--mql-expect reader ?\()
  (let ((args (clutch-mongodb--mql-parse-args-until-end
               reader
               ?\))))
    (pcase name
      ("ObjectId"
       (unless (and (= (length args) 1)
                    (stringp (car args)))
         (signal 'clutch-db-error
                 (list "ObjectId() expects one hex string")))
       (mongodb-object-id (car args)))
      ("ISODate"
       (unless (and (= (length args) 1)
                    (stringp (car args)))
         (signal 'clutch-db-error
                 (list "ISODate() expects one ISO-8601 string")))
       (mongodb-datetime
        (clutch-mongodb--mql-iso-date-millis (car args))))
      (_
       (signal 'clutch-db-error
               (list (format "Unsupported MongoDB constructor: %s" name)))))))

(defun clutch-mongodb--mql-parse-identifier-value (reader)
  "Parse an identifier-like value from READER."
  (let ((token (clutch-mongodb--mql-parse-identifier reader)))
    (clutch-mongodb--mql-skip-space reader)
    (cond
     ((eq (clutch-mongodb--mql-peek reader) ?\()
      (clutch-mongodb--mql-parse-constructor token reader))
     ((string= token "true") t)
     ((string= token "false") :false)
     ((string= token "null") nil)
     (t token))))

(defun clutch-mongodb--mql-parse-value (reader)
  "Parse one MQL literal value from READER."
  (clutch-mongodb--mql-skip-space reader)
  (pcase (clutch-mongodb--mql-peek reader)
    ((or ?\" ?\') (clutch-mongodb--mql-parse-string reader))
    (?{ (clutch-mongodb--mql-parse-object reader))
    (?\[ (clutch-mongodb--mql-parse-array reader))
    (?/ (signal 'clutch-db-error
                (list "MongoDB regex literals are not supported; use {$regex: \"pattern\"}")))
    ((or ?- (guard (and (clutch-mongodb--mql-peek reader)
                        (<= ?0 (clutch-mongodb--mql-peek reader))
                        (<= (clutch-mongodb--mql-peek reader) ?9))))
     (clutch-mongodb--mql-parse-number reader))
    (_ (clutch-mongodb--mql-parse-identifier-value reader))))

(defun clutch-mongodb--mql-parse-args-until-end (reader end-char)
  "Parse comma-separated args in READER until END-CHAR."
  (let (args done)
    (while (not done)
      (clutch-mongodb--mql-skip-space reader)
      (if (if end-char
              (eq (clutch-mongodb--mql-peek reader) end-char)
            (clutch-mongodb--mql-end-p reader))
          (progn
            (when end-char
              (clutch-mongodb--mql-read reader))
            (setq done t))
        (push (clutch-mongodb--mql-parse-value reader) args)
        (clutch-mongodb--mql-skip-space reader)
        (cond
         ((eq (clutch-mongodb--mql-peek reader) ?,)
          (clutch-mongodb--mql-read reader))
         ((if end-char
              (eq (clutch-mongodb--mql-peek reader) end-char)
            (clutch-mongodb--mql-end-p reader))
          nil)
         (t
          (signal 'clutch-db-error
                  (list "Expected `,' or closing delimiter in MongoDB arguments"))))))
    (nreverse args)))

(defun clutch-mongodb--mql-parse-args (text)
  "Parse MongoDB helper argument TEXT."
  (let ((reader (make-clutch-mongodb--mql-reader
                 :text text
                 :pos 0)))
    (prog1 (clutch-mongodb--mql-parse-args-until-end reader nil)
      (clutch-mongodb--mql-skip-space reader)
      (unless (clutch-mongodb--mql-end-p reader)
        (signal 'clutch-db-error
                (list "Trailing text in MongoDB arguments"))))))

(defun clutch-mongodb--split-statements (code)
  "Split MongoDB CODE on top-level semicolons."
  (let ((start 0)
        (depth 0)
        quote
        escape
        parts
        (index 0))
    (while (< index (length code))
      (let ((char (aref code index)))
        (cond
         (escape
          (setq escape nil)
          (cl-incf index))
         ((and quote (eq char ?\\))
          (setq escape t)
          (cl-incf index))
         (quote
          (when (eq char quote)
            (setq quote nil))
          (cl-incf index))
         ((memq char '(?\" ?\'))
          (setq quote char)
          (cl-incf index))
         ((memq char '(?\( ?{ ?\[))
          (cl-incf depth)
          (cl-incf index))
         ((memq char '(?\) ?} ?\]))
          (cl-decf depth)
          (cl-incf index))
         ((and (eq char ?\;) (zerop depth))
          (push (string-trim (substring code start index)) parts)
          (setq start (1+ index))
          (cl-incf index))
         (t
          (cl-incf index)))))
    (push (string-trim (substring code start)) parts)
    (seq-filter (lambda (part) (not (string-empty-p part)))
                (nreverse parts))))

(defun clutch-mongodb--matching-paren (text open-pos)
  "Return matching close paren for TEXT at OPEN-POS."
  (let ((depth 0)
        quote
        escape
        found
        (index open-pos))
    (while (and (< index (length text))
                (not found))
      (let ((char (aref text index)))
        (cond
         (escape
          (setq escape nil)
          (cl-incf index))
         ((and quote (eq char ?\\))
          (setq escape t)
          (cl-incf index))
         (quote
          (when (eq char quote)
            (setq quote nil))
          (cl-incf index))
         ((memq char '(?\" ?\'))
          (setq quote char)
          (cl-incf index))
         ((eq char ?\()
          (cl-incf depth)
          (cl-incf index))
         ((eq char ?\))
          (cl-decf depth)
          (when (zerop depth)
            (setq found index))
          (cl-incf index))
         (t
          (cl-incf index)))))
    (or found
        (signal 'clutch-db-error
                (list "Unclosed MongoDB helper call")))))

(defun clutch-mongodb--parse-call-args (text open-pos)
  "Return (ARGS . CLOSE-POS) for TEXT call starting at OPEN-POS."
  (let* ((close (clutch-mongodb--matching-paren text open-pos))
         (inside (substring text (1+ open-pos) close)))
    (cons (clutch-mongodb--mql-parse-args inside) close)))

(defun clutch-mongodb--parse-method-call (text pos collection)
  "Parse a collection method call in TEXT at POS for COLLECTION."
  (unless (and (< pos (length text))
               (eq (aref text pos) ?.))
    (signal 'clutch-db-error
            (list "Expected MongoDB collection method call")))
  (let* ((method-start (1+ pos))
         (open (string-match-p "[[:space:]]*(" text method-start))
         method args close chain)
    (unless open
      (signal 'clutch-db-error
              (list "Expected MongoDB collection method arguments")))
    (setq method (string-trim (substring text method-start open)))
    (pcase-let ((`(,parsed-args . ,close-pos)
                 (clutch-mongodb--parse-call-args text open)))
      (setq args parsed-args
            close close-pos))
    (setq chain (clutch-mongodb--parse-helper-chain
                 (substring text (1+ close)) method))
    (list :collection collection :method method :args args :chain chain)))

(defun clutch-mongodb--parse-helper-chain (text collection-method)
  "Parse helper chain suffix TEXT supported by COLLECTION-METHOD."
  (let ((tail (string-trim text))
        (allowed-methods
         (pcase collection-method
           ("find" '("sort" "skip" "limit" "maxTimeMS"
                     "allowDiskUse" "explain"))
           ("aggregate" '("maxTimeMS" "allowDiskUse" "explain"))))
        chain)
    (while (not (string-empty-p tail))
      (unless (string-prefix-p "." tail)
        (signal 'clutch-db-error
                (list "Unsupported MongoDB helper chain")))
      (let* ((method-start 1)
             (open (string-match-p "[[:space:]]*(" tail method-start)))
        (unless open
          (signal 'clutch-db-error
                  (list "Expected MongoDB chained helper arguments")))
        (let ((method (string-trim (substring tail method-start open))))
          (pcase-let ((`(,args . ,close)
                       (clutch-mongodb--parse-call-args tail open)))
            (unless (member method allowed-methods)
              (signal 'clutch-db-error
                      (list (format "Unsupported MongoDB %s() chain helper: %s"
                                    collection-method method))))
            (pcase method
              ((or "limit" "skip")
               (unless (and (= (length args) 1)
                            (integerp (car args)))
                 (signal 'clutch-db-error
                         (list (format "%s() expects one integer argument"
                                       method))))
               (push (cons method (car args)) chain))
              ("maxTimeMS"
               (unless (= (length args) 1)
                 (signal 'clutch-db-error
                         (list (format "%s() expects one integer argument"
                                       method))))
               (push (cons method
                           (clutch-mongodb--mql-nonnegative-integer-arg
                            (car args)
                            method))
                     chain))
              ("allowDiskUse"
               (push (cons method
                           (clutch-mongodb--mql-boolean-option-arg
                            args
                            method))
                     chain))
              ("sort"
               (unless (and (= (length args) 1)
                            (mongodb-document-p (car args)))
                 (signal 'clutch-db-error
                         (list "sort() expects one document argument")))
               (push (cons method (car args)) chain))
              ("explain"
               (unless (<= (length args) 1)
                 (signal 'clutch-db-error
                         (list "explain() expects optional verbosity")))
               (unless (or (null args)
                           (stringp (car args))
                           (memq (car args) '(t :false)))
                 (signal 'clutch-db-error
                         (list "explain() verbosity must be a string or boolean")))
               (push (cons method (car args)) chain))
              (_
               (signal 'clutch-db-error
                       (list (format "Unsupported MongoDB chained helper: %s"
                                     method)))))
            (setq tail (string-trim (substring tail (1+ close))))))))
    (nreverse chain)))

(defun clutch-mongodb--next-db-member-separator (text start)
  "Return the next `.' or `(' position in TEXT from START.
When both are present, return the earlier one."
  (let ((dot (cl-position ?. text :start start))
        (open (cl-position ?\( text :start start)))
    (cond
     ((and dot open) (min dot open))
     (dot dot)
     (open open)
     (t (length text)))))

(defun clutch-mongodb--options-from-chain (chain methods)
  "Return MongoDB command option pairs parsed from CHAIN for METHODS."
  (delq nil
        (mapcar
         (lambda (pair)
           (let ((method (car pair))
                 (value (cdr pair)))
             (when (member method methods)
               (cons method value))))
         chain)))

(defun clutch-mongodb--merge-options (base extra)
  "Return option document merged from BASE document and EXTRA alist."
  (let ((pairs (append
                (cl-remove-if (lambda (pair)
                                (assoc (car pair) extra))
                              (and base (mongodb-document-elements base)))
                extra)))
    (and pairs (mongodb-document pairs))))

(defun clutch-mongodb--explain-verbosity (chain)
  "Return explain verbosity requested by helper CHAIN, or nil."
  (cdr (assoc "explain" chain)))

(defun clutch-mongodb--parse-db-member-call (text rest-pos)
  "Parse a MongoDB DB member call in TEXT starting at REST-POS."
  (cond
   ((string-prefix-p "getCollection(" (substring text rest-pos))
    (let* ((open (+ rest-pos (length "getCollection")))
           (parsed (clutch-mongodb--parse-call-args text open))
           (args (car parsed))
           (close (cdr parsed)))
      (unless (and (= (length args) 1)
                   (stringp (car args)))
        (signal 'clutch-db-error
                (list "db.getCollection() expects one collection name string")))
      (clutch-mongodb--parse-method-call text (1+ close) (car args))))
   (t
    (let* ((token-end
            (clutch-mongodb--next-db-member-separator text rest-pos))
           (token (substring text rest-pos token-end)))
      (if (and (< token-end (length text))
               (eq (aref text token-end) ?\())
          (pcase-let ((`(,args . ,close)
                       (clutch-mongodb--parse-call-args text token-end)))
            (unless (string-empty-p (string-trim (substring text (1+ close))))
              (signal 'clutch-db-error
                      (list "Unsupported MongoDB helper chain")))
            (list :db-method token :args args))
        (clutch-mongodb--parse-method-call text token-end token))))))

(defun clutch-mongodb--parse-db-call (statement)
  "Parse one MongoDB shell helper STATEMENT."
  (let ((text (string-trim statement)))
    (unless (string-prefix-p "db." text)
      (signal 'clutch-db-error
              (list "Native MongoDB supports db.* helper calls, not arbitrary JavaScript")))
    (clutch-mongodb--parse-db-member-call text 3)))

(defun clutch-mongodb--mql-document-arg (value helper position)
  "Return VALUE as a MongoDB document argument for HELPER at POSITION."
  (unless (mongodb-document-p value)
    (signal 'clutch-db-error
            (list (format "%s() argument %d must be a document"
                          helper position))))
  value)

(defun clutch-mongodb--mql-optional-document-arg (value helper position)
  "Return optional VALUE as a MongoDB document argument for HELPER at POSITION."
  (when value
    (clutch-mongodb--mql-document-arg value helper position)))

(defun clutch-mongodb--delete-filter-arg (args method)
  "Return the required delete filter document from ARGS for METHOD."
  (unless (= (length args) 1)
    (signal 'clutch-db-error
            (list (format "%s() expects one filter document" method))))
  (clutch-mongodb--mql-document-arg (car args) method 1))

(defun clutch-mongodb--find-arguments (args chain &optional single)
  "Return MongoDB find arguments parsed from ARGS and CHAIN.
When SINGLE is non-nil, force a single-result limit and ignore cursor paging
chain options."
  (unless (<= (length args) 2)
    (signal 'clutch-db-error
            (list "find() expects optional filter and projection documents")))
  (unless (and (integerp clutch-mongodb-find-result-limit)
               (> clutch-mongodb-find-result-limit 0))
    (user-error "MongoDB find result limit must be a positive integer"))
  (let ((requested-limit (cdr (assoc "limit" chain))))
    (when (and requested-limit
               (or (not (integerp requested-limit))
                   (< requested-limit 1)
                   (> requested-limit clutch-mongodb-find-result-limit)))
      (signal 'clutch-db-error
              (list (format "MongoDB find limit must be between 1 and %d"
                            clutch-mongodb-find-result-limit))))
    (list (if (car args)
              (clutch-mongodb--mql-document-arg (car args) "find" 1)
            (mongodb-document nil))
          (clutch-mongodb--mql-optional-document-arg
           (nth 1 args) "find" 2)
          (if single
              1
            (or requested-limit clutch-mongodb-find-result-limit))
          (unless single (cdr (assoc "skip" chain)))
          (unless single (cdr (assoc "sort" chain)))
          (unless single
            (clutch-mongodb--options-from-chain
             chain '("maxTimeMS" "allowDiskUse"))))))

(defun clutch-mongodb--find-command (collection args chain &optional single)
  "Return a MongoDB find command for COLLECTION from ARGS, CHAIN, and SINGLE."
  (pcase-let ((`(,filter ,projection ,limit ,skip ,sort ,options)
               (clutch-mongodb--find-arguments args chain single)))
    (mongodb-find-command collection filter projection limit skip sort options)))

(defun clutch-mongodb--aggregate-options (args chain)
  "Return MongoDB aggregate options parsed from ARGS and CHAIN."
  (unless (and (<= 1 (length args) 2)
               (vectorp (car args)))
    (signal 'clutch-db-error
            (list "aggregate() expects a pipeline array and optional options document")))
  (when (and (nth 1 args)
             (not (mongodb-document-p (nth 1 args))))
    (signal 'clutch-db-error
            (list "aggregate() options argument must be a document")))
  (clutch-mongodb--merge-options
   (nth 1 args)
   (clutch-mongodb--options-from-chain
    chain '("allowDiskUse" "maxTimeMS"))))

(defun clutch-mongodb--aggregate-command (collection args chain)
  "Return a MongoDB aggregate command for COLLECTION from ARGS and CHAIN."
  (mongodb-aggregate-command
   collection
   (car args)
   (clutch-mongodb--aggregate-options args chain)))

(defun clutch-mongodb--execute-collection-method
    (conn collection method args &optional chain)
  "Execute collection METHOD on COLLECTION with ARGS on CONN.
CHAIN contains parsed cursor helper calls, when present."
  (let ((client (clutch-mongodb-conn-client conn))
        (database (clutch-mongodb-conn-database conn)))
    (pcase method
      ("find"
       (pcase-let ((`(,filter ,projection ,limit ,skip ,sort ,options)
                    (clutch-mongodb--find-arguments args chain)))
         (if (assoc "explain" chain)
             (mongodb-explain
              client database
              (mongodb-find-command
               collection filter projection limit skip sort options)
              (clutch-mongodb--explain-verbosity chain))
           (mongodb-find
            client database collection filter projection limit skip sort options))))
      ("findOne"
       (pcase-let ((`(,filter ,projection ,limit ,skip ,sort ,options)
                    (clutch-mongodb--find-arguments args chain t)))
         (car (mongodb-find
               client database collection filter projection limit skip sort options))))
      ("countDocuments"
       (unless (<= (length args) 2)
         (signal 'clutch-db-error
                 (list "countDocuments() expects optional filter and options")))
       (mongodb-count-documents
        client database collection
        (if (car args)
            (clutch-mongodb--mql-document-arg (car args) method 1)
          (mongodb-document nil))
        (clutch-mongodb--mql-optional-document-arg
         (nth 1 args) method 2)))
      ("distinct"
       (unless (and (<= 1 (length args) 3)
                    (stringp (nth 0 args)))
         (signal 'clutch-db-error
                 (list "distinct() expects field name, optional filter, and optional options")))
       (mongodb-distinct
        client database collection
        (nth 0 args)
        (clutch-mongodb--mql-optional-document-arg
         (nth 1 args) method 2)
        (clutch-mongodb--mql-optional-document-arg
         (nth 2 args) method 3)))
      ("aggregate"
       (if (assoc "explain" chain)
           (mongodb-explain
            client database
            (clutch-mongodb--aggregate-command collection args chain)
            (clutch-mongodb--explain-verbosity chain))
         (mongodb-aggregate
          client database collection
          (car args)
          (clutch-mongodb--aggregate-options args chain))))
      ("insertOne"
       (unless (= (length args) 1)
         (signal 'clutch-db-error
                 (list "insertOne() expects one document")))
       (mongodb-insert
        client database collection
        (clutch-mongodb--mql-document-arg (car args) method 1)))
      ("insertMany"
       (unless (= (length args) 1)
         (signal 'clutch-db-error
                 (list "insertMany() expects one document array")))
       (let ((documents (car args)))
         (unless (and (vectorp documents)
                      (cl-every #'mongodb-document-p (append documents nil)))
           (signal 'clutch-db-error
                   (list "insertMany() expects one document array")))
         (mongodb-insert client database collection documents)))
      ("deleteOne"
       (mongodb-delete
        client database collection
        (clutch-mongodb--delete-filter-arg args method)
        nil))
      ("updateOne"
       (unless (<= 2 (length args) 3)
         (signal 'clutch-db-error
                 (list "updateOne() expects filter, update, and optional options")))
       (mongodb-update
        client database collection
        (clutch-mongodb--mql-document-arg (nth 0 args) method 1)
        (clutch-mongodb--mql-document-arg (nth 1 args) method 2)
        nil
        (clutch-mongodb--mql-optional-document-arg
         (nth 2 args) method 3)))
      ("replaceOne"
       (unless (<= 2 (length args) 3)
         (signal 'clutch-db-error
                 (list "replaceOne() expects filter, replacement, and optional options")))
       (mongodb-update
        client database collection
        (clutch-mongodb--mql-document-arg (nth 0 args) method 1)
        (clutch-mongodb--mql-document-arg (nth 1 args) method 2)
        nil
        (clutch-mongodb--mql-optional-document-arg
         (nth 2 args) method 3)))
      (_
       (signal 'clutch-db-error
               (list (format "Unsupported MongoDB collection helper: %s"
                             method)))))))

(defun clutch-mongodb--eval-one (conn statement)
  "Evaluate one parsed MongoDB helper STATEMENT on CONN."
  (let ((call (clutch-mongodb--parse-db-call statement)))
    (if-let* ((method (plist-get call :db-method)))
        (if (string= method "runCommand")
            (let ((args (plist-get call :args)))
              (unless (and (= (length args) 1)
                           (mongodb-document-p (car args)))
                (signal 'clutch-db-error
                        (list "db.runCommand() expects one command document")))
              (mongodb-command
               (clutch-mongodb-conn-client conn)
               (clutch-mongodb-conn-database conn)
               (car args)))
          (signal 'clutch-db-error
                  (list (format "Unsupported MongoDB db helper: %s" method))))
      (clutch-mongodb--execute-collection-method
       conn
       (plist-get call :collection)
       (plist-get call :method)
       (plist-get call :args)
       (plist-get call :chain)))))

(defun clutch-mongodb--eval (conn code)
  "Evaluate supported MongoDB shell helper CODE on CONN."
  (clutch-mongodb--with-mongodb-errors
    (let (value)
      (dolist (statement (clutch-mongodb--split-statements code))
        (setq value (clutch-mongodb--eval-one conn statement)))
      value)))

(defun clutch-mongodb--single-helper-call (query purpose)
  "Parse QUERY as one MongoDB helper call for PURPOSE."
  (let ((statements (clutch-mongodb--split-statements query)))
    (unless (= (length statements) 1)
      (signal 'clutch-db-error
              (list (format "MongoDB %s expects exactly one helper call"
                            purpose))))
    (clutch-mongodb--parse-db-call (car statements))))

(defun clutch-mongodb--last-helper-call (query)
  "Return the parsed last MongoDB helper call in QUERY, or nil."
  (when-let* ((statements (clutch-mongodb--split-statements query)))
    (clutch-mongodb--parse-db-call (car (last statements)))))

(defun clutch-mongodb--find-document-key (value key)
  "Return the first value for KEY found recursively in MongoDB VALUE."
  (cond
   ((or (mongodb-document-p value)
        (clutch-mongodb--alist-p value))
    (or (cdr (assoc key (clutch-mongodb--document-elements value)))
        (cl-loop for pair in (clutch-mongodb--document-elements value)
                 thereis (clutch-mongodb--find-document-key (cdr pair) key))))
   ((vectorp value)
    (cl-loop for item across value
             thereis (clutch-mongodb--find-document-key item key)))
   ((listp value)
    (cl-loop for item in value
             thereis (clutch-mongodb--find-document-key item key)))))

(defun clutch-mongodb--document-has-value-p (value key expected)
  "Return non-nil if VALUE has recursive KEY equal to EXPECTED."
  (cond
   ((or (mongodb-document-p value)
        (clutch-mongodb--alist-p value))
    (or (equal (cdr (assoc key (clutch-mongodb--document-elements value)))
               expected)
        (cl-loop for pair in (clutch-mongodb--document-elements value)
                 thereis (clutch-mongodb--document-has-value-p
                          (cdr pair) key expected))))
   ((vectorp value)
    (cl-loop for item across value
             thereis (clutch-mongodb--document-has-value-p item key expected)))
   ((listp value)
    (cl-loop for item in value
             thereis (clutch-mongodb--document-has-value-p item key expected)))))

(defun clutch-mongodb--explain-summary (explain)
  "Return a compact JSON-ready summary for MongoDB EXPLAIN output."
  (let* ((winning-plan (clutch-mongodb--find-document-key explain "winningPlan"))
         (winning-stage (or (and winning-plan
                                  (clutch-mongodb--find-document-key
                                   winning-plan "stage"))
                            (clutch-mongodb--find-document-key explain "stage")))
         (index-name (or (and winning-plan
                              (clutch-mongodb--find-document-key
                               winning-plan "indexName"))
                         (clutch-mongodb--find-document-key explain "indexName"))))
    (delq nil
          `(("winningStage" . ,winning-stage)
            ,(when index-name
               `("indexName" . ,index-name))
            ("collectionScan" . ,(if (clutch-mongodb--document-has-value-p
                                      explain "stage" "COLLSCAN")
                                     t
                                   :false))
            ,(when-let* ((value (clutch-mongodb--find-document-key
                                 explain "nReturned")))
               `("nReturned" . ,value))
            ,(when-let* ((value (clutch-mongodb--find-document-key
                                 explain "totalKeysExamined")))
               `("totalKeysExamined" . ,value))
            ,(when-let* ((value (clutch-mongodb--find-document-key
                                 explain "totalDocsExamined")))
               `("totalDocsExamined" . ,value))
            ,(when-let* ((value (clutch-mongodb--find-document-key
                                 explain "executionTimeMillis")))
               `("executionTimeMillis" . ,value))))))

(defun clutch-mongodb--explain-call (conn call)
  "Return MongoDB explain value for parsed helper CALL on CONN."
  (let* ((method (plist-get call :method))
         (collection (plist-get call :collection))
         (args (plist-get call :args))
         (chain (plist-get call :chain))
         (database (clutch-mongodb-conn-database conn)))
    (unless (and collection
                 (member method '("find" "findOne" "aggregate")))
      (signal 'clutch-db-error
              (list "MongoDB explain supports find(), findOne(), and aggregate() helpers")))
    (mongodb-explain
     (clutch-mongodb-conn-client conn)
     database
     (if (string= method "aggregate")
         (clutch-mongodb--aggregate-command collection args chain)
       (clutch-mongodb--find-command
        collection args chain (string= method "findOne")))
     (or (clutch-mongodb--explain-verbosity chain)
         "executionStats"))))

;;;; Result conversion

(defun clutch-mongodb--alist-p (value)
  "Return non-nil when VALUE is a JSON object alist."
  (and (consp value)
       (or (null value)
           (consp (car value)))
       (not (listp (caar value)))))

(defun clutch-mongodb--document-list-p (value)
  "Return non-nil when VALUE is a list of JSON object alists."
  (and (listp value)
       (or (null value)
           (cl-every #'clutch-mongodb--alist-p value))))

(defun clutch-mongodb--scalar-p (value)
  "Return non-nil when VALUE can be displayed as a scalar cell."
  (or (null value)
      (stringp value)
      (numberp value)
      (eq value t)
      (eq value :false)))

(defun clutch-mongodb--column-category (value)
  "Return Clutch type category for MongoDB VALUE."
  (cond
   ((numberp value) 'numeric)
   ((clutch-mongodb--scalar-p value) 'text)
   (t 'json)))

(defun clutch-mongodb--value-type-name (value)
  "Return a compact sampled BSON type name for VALUE."
  (cond
   ((null value) "null")
   ((or (eq value t) (eq value :false)) "bool")
   ((numberp value) "number")
   ((stringp value) "string")
   ((vectorp value) "array")
   ((mongodb-document-p value) "object")
   ((clutch-mongodb--alist-p value) "object")
   ((listp value) "array")
   (t "value")))

(defun clutch-mongodb--field-type-category (values)
  "Return a Clutch type category for sampled field VALUES."
  (cond
   ((seq-some (lambda (value)
                (member (clutch-mongodb--value-type-name value)
                        '("array" "object")))
              values)
    'json)
   ((and values (cl-every #'numberp values)) 'numeric)
   (t 'text)))

(defun clutch-mongodb--display-value (value)
  "Return display cell value for MongoDB VALUE."
  (cond
   ((eq value :false) "false")
   ((eq value t) "true")
   ((clutch-mongodb--scalar-p value) value)
   (t (clutch-mongodb--json-encode-text value))))

(defun clutch-mongodb--json-encodable (value)
  "Return VALUE recursively normalized for `json-encode'."
  (cond
   ((eq value :false) json-false)
   ((mongodb-document-p value)
    (clutch-mongodb--json-encodable (mongodb-document-elements value)))
   ((clutch-mongodb--alist-p value)
    (mapcar (lambda (pair)
              (cons (car pair)
                    (clutch-mongodb--json-encodable (cdr pair))))
            value))
   ((vectorp value)
    (vconcat (mapcar #'clutch-mongodb--json-encodable (append value nil))))
   ((listp value)
    (vconcat (mapcar #'clutch-mongodb--json-encodable value)))
   (t value)))

(defun clutch-mongodb--json-encode-text (value)
  "Return VALUE encoded as JSON text for MongoDB result display."
  (json-encode (clutch-mongodb--json-encodable value)))

(defun clutch-mongodb--document-elements (document)
  "Return top-level elements from MongoDB DOCUMENT."
  (cond
   ((mongodb-document-p document) (mongodb-document-elements document))
   ((clutch-mongodb--alist-p document) document)
   (t (signal 'clutch-db-error
              (list "MongoDB expected a metadata document")))))

(defun clutch-mongodb--document-value (document key)
  "Return KEY's value from MongoDB DOCUMENT."
  (cdr (assoc key (clutch-mongodb--document-elements document))))

(defun clutch-mongodb--document-pair (document key)
  "Return KEY's top-level pair from MongoDB DOCUMENT, or nil."
  (assoc key (clutch-mongodb--document-elements document)))

(defun clutch-mongodb--document-id-filter (document action)
  "Return an _id filter document for DOCUMENT during ACTION."
  (if-let* ((pair (clutch-mongodb--document-pair document "_id")))
      (list (cons "_id" (cdr pair)))
    (user-error "Cannot build MongoDB %s: document has no _id" action)))

(defun clutch-mongodb--document-set-fields (document fields action)
  "Return a MongoDB $set document from DOCUMENT FIELDS for ACTION."
  (when (member "_id" fields)
    (user-error "Cannot build MongoDB %s: _id is not writable" action))
  (let ((pairs
         (cl-loop for field in fields
                  for pair = (clutch-mongodb--document-pair document field)
                  unless pair
                  do (user-error
                      "Cannot build MongoDB %s: field %s is absent in document"
                      action field)
                  collect (cons field (cdr pair)))))
    (unless pairs
      (user-error "Cannot build MongoDB %s: no fields selected" action))
    pairs))

(defun clutch-mongodb--helper-call (collection method args)
  "Return MongoDB helper text for COLLECTION METHOD and ARGS."
  (format "db.getCollection(%s).%s(%s);"
          (clutch-mongodb--json-encode-text collection)
          method
          (mapconcat #'clutch-mongodb--json-encode-text args ", ")))

(defun clutch-mongodb--document-mutation-snippet
    (action collection documents &optional fields)
  "Return MongoDB helper snippets for ACTION on COLLECTION and DOCUMENTS.
FIELDS is an optional list of top-level field names for update snippets."
  (pcase action
    ('insert-one
     (cl-loop for document in documents
              collect (clutch-mongodb--helper-call
                       collection "insertOne" (list document))))
    ('insert-many
     (list (clutch-mongodb--helper-call
            collection "insertMany" (list (vconcat documents)))))
    ('replace-one
     (cl-loop for document in documents
              collect (clutch-mongodb--helper-call
                       collection "replaceOne"
                       (list (clutch-mongodb--document-id-filter
                              document "replaceOne")
                             document))))
    ('delete-one
     (cl-loop for document in documents
              collect (clutch-mongodb--helper-call
                       collection "deleteOne"
                       (list (clutch-mongodb--document-id-filter
                              document "deleteOne")))))
    ('update-one-set
     (cl-loop for document in documents
              collect (clutch-mongodb--helper-call
                       collection "updateOne"
                       (list (clutch-mongodb--document-id-filter
                              document "updateOne")
                             (list
                              (cons "$set"
                                    (clutch-mongodb--document-set-fields
                                     document fields "updateOne")))))))
    (_
     (user-error "Unsupported MongoDB document mutation action: %s" action))))

(defun clutch-mongodb--field-type-counts (values)
  "Return sorted BSON type count objects for sampled VALUES."
  (let ((counts (make-hash-table :test 'equal)))
    (dolist (value values)
      (let ((type (clutch-mongodb--value-type-name value)))
        (puthash type (1+ (or (gethash type counts) 0)) counts)))
    (vconcat
     (sort
      (cl-loop for type being the hash-keys of counts
               using (hash-values count)
               collect `(("type" . ,type)
                         ("count" . ,count)))
      (lambda (a b)
        (let ((count-a (cdr (assoc "count" a)))
              (count-b (cdr (assoc "count" b))))
          (if (= count-a count-b)
              (string< (cdr (assoc "type" a))
                       (cdr (assoc "type" b)))
            (> count-a count-b))))))))

(defun clutch-mongodb--sample-field-type (values)
  "Return sampled BSON type label for VALUES."
  (let ((types nil))
    (dolist (value values)
      (let ((type (clutch-mongodb--value-type-name value)))
        (unless (member type types)
          (push type types))))
    (format "BSON<%s>"
            (string-join (nreverse types) "|"))))

(defun clutch-mongodb--sample-field-comment (present total)
  "Return a field comment for PRESENT out of TOTAL sampled documents."
  (when (and (> total 0) (< present total))
    (format "present in %d/%d sampled documents" present total)))

(defun clutch-mongodb--index-direction (value)
  "Return a display direction for a MongoDB index key VALUE."
  (cond
   ((eq value 1) "ASC")
   ((eq value -1) "DESC")
   ((stringp value) (upcase value))
   (t (format "%s" value))))

(defun clutch-mongodb--index-key-details (key-document)
  "Return object-detail rows for MongoDB index KEY-DOCUMENT."
  (cl-loop for pair in (mongodb-document-elements key-document)
           for position from 1
           collect (list :name (car pair)
                         :position position
                         :descend (clutch-mongodb--index-direction
                                   (cdr pair)))))

(defun clutch-mongodb--index-entry (conn collection document)
  "Return a Clutch index entry for MongoDB index DOCUMENT on COLLECTION in CONN."
  (let ((name (clutch-mongodb--document-value document "name"))
        (key (clutch-mongodb--document-value document "key"))
        (database (clutch-mongodb-conn-database conn)))
    (list :name name
          :type "INDEX"
          :schema database
          :source-schema database
          :target-table collection
          :identity (format "%s.%s" collection name)
          :unique (eq (clutch-mongodb--document-value document "unique") t)
          :key key
          :definition document)))

(defun clutch-mongodb--index-json (document &optional stats)
  "Return a JSON-serializable index insight object from DOCUMENT and STATS."
  (let* ((name (clutch-mongodb--document-value document "name"))
         (key (clutch-mongodb--document-value document "key"))
         (unique-pair (assoc "unique"
                             (clutch-mongodb--document-elements document)))
         (accesses (and stats
                        (clutch-mongodb--document-value stats "accesses"))))
    (delq nil
          `(("name" . ,name)
            ,(when key
               `("key" . ,(clutch-mongodb--json-encodable key)))
            ,(when unique-pair
               `("unique" . ,(if (eq (cdr unique-pair) t)
                                 t
                               :false)))
            ,(when-let* ((type (clutch-mongodb--document-value document "type")))
               `("type" . ,type))
            ,(when-let* ((expire (clutch-mongodb--document-value
                                  document "expireAfterSeconds")))
               `("expireAfterSeconds" . ,expire))
            ,(when accesses
               `("usage" . ,(clutch-mongodb--json-encodable accesses)))
            ,(when stats
               `("host" . ,(clutch-mongodb--document-value stats "host")))))))

(defun clutch-mongodb--index-document (conn entry)
  "Return the MongoDB index document for object ENTRY on CONN."
  (or (plist-get entry :definition)
      (when-let* ((collection (plist-get entry :target-table))
                  (name (plist-get entry :name)))
        (seq-find
         (lambda (document)
           (equal (clutch-mongodb--document-value document "name") name))
         (mongodb-list-indexes
          (clutch-mongodb-conn-client conn)
          (clutch-mongodb-conn-database conn)
          collection)))))

(defun clutch-mongodb--ordered-keys (docs)
  "Return stable top-level keys for DOCS, keeping _id first when present."
  (let ((keys nil))
    (dolist (doc docs)
      (dolist (pair doc)
        (let ((key (car pair)))
          (unless (member key keys)
            (push key keys)))))
    (setq keys (nreverse keys))
    (if (member "_id" keys)
        (cons "_id" (remove "_id" keys))
      keys)))

(defun clutch-mongodb--schema-sample-limit ()
  "Return the MongoDB schema sample size."
  (unless (and (integerp clutch-mongodb-schema-sample-size)
               (> clutch-mongodb-schema-sample-size 0))
    (signal 'clutch-db-error
            (list "MongoDB schema sample size must be a positive integer")))
  clutch-mongodb-schema-sample-size)

(defun clutch-mongodb--sample-documents (conn collection)
  "Return sampled documents from COLLECTION on CONN."
  (clutch-mongodb--with-mongodb-errors
    (let ((value (mongodb-find
                  (clutch-mongodb-conn-client conn)
                  (clutch-mongodb-conn-database conn)
                  collection nil nil (clutch-mongodb--schema-sample-limit))))
      (cond
       ((clutch-mongodb--document-list-p value) value)
       (t (signal 'clutch-db-error
                  (list "MongoDB schema sampling returned a non-document result")))))))

(defun clutch-mongodb--profile-stat (stats path)
  "Return the mutable profile stat for PATH in STATS."
  (or (gethash path stats)
      (puthash path
               (list :path path
                     :present 0
                     :values nil
                     :types (make-hash-table :test 'equal)
                     :top-values (make-hash-table :test 'equal)
                     :examples nil
                     :numeric-min nil
                     :numeric-max nil)
               stats)))

(defun clutch-mongodb--profile-value-key (value)
  "Return a stable hash key for sampled profile VALUE."
  (cond
   ((eq value :false) "false")
   ((eq value t) "true")
   ((null value) "null")
   ((stringp value) (concat "s:" value))
   ((numberp value) (format "n:%s" value))
   (t (clutch-mongodb--json-encode-text value))))

(defun clutch-mongodb--profile-top-values (stat)
  "Return top sampled scalar values for profile STAT."
  (vconcat
   (cl-subseq
    (sort
     (cl-loop with counts = (plist-get stat :top-values)
              for _key being the hash-keys of counts
              using (hash-values payload)
              collect `(("value" . ,(plist-get payload :value))
                        ("count" . ,(plist-get payload :count))))
     (lambda (a b)
       (> (cdr (assoc "count" a))
         (cdr (assoc "count" b)))))
    0
    (min 5 (hash-table-count (plist-get stat :top-values))))))

(defun clutch-mongodb--profile-record-value (stats seen path value)
  "Record sampled VALUE for PATH in STATS, tracking document-level SEEN paths."
  (let* ((stat (clutch-mongodb--profile-stat stats path))
         (type (clutch-mongodb--value-type-name value))
         (types (plist-get stat :types)))
    (unless (gethash path seen)
      (puthash path t seen)
      (setq stat (plist-put stat :present
                            (1+ (plist-get stat :present)))))
    (push value (plist-get stat :values))
    (puthash type (1+ (or (gethash type types) 0)) types)
    (when (numberp value)
      (setq stat
            (plist-put stat :numeric-min
                       (if (numberp (plist-get stat :numeric-min))
                           (min (plist-get stat :numeric-min) value)
                         value)))
      (setq stat
            (plist-put stat :numeric-max
                       (if (numberp (plist-get stat :numeric-max))
                           (max (plist-get stat :numeric-max) value)
                         value))))
    (when (clutch-mongodb--scalar-p value)
      (let* ((examples (plist-get stat :examples))
             (key (clutch-mongodb--profile-value-key value))
             (top-values (plist-get stat :top-values))
             (payload (or (gethash key top-values)
                          (list :value (clutch-mongodb--json-encodable value)
                                :count 0))))
        (when (< (length examples) 3)
          (setq stat (plist-put stat :examples
                                (append examples
                                        (list (clutch-mongodb--json-encodable
                                               value))))))
        (puthash key
                 (plist-put payload :count
                            (1+ (plist-get payload :count)))
                 top-values)))
    (puthash path stat stats)))

(defun clutch-mongodb--extended-json-wrapper-p (value)
  "Return non-nil if VALUE is an Extended JSON scalar wrapper."
  (and (clutch-mongodb--alist-p value)
       (= (length value) 1)
       (stringp (caar value))
       (string-prefix-p "$" (caar value))))

(defun clutch-mongodb--profile-stats-for-docs (docs)
  "Return sorted field profile stat plists sampled from DOCS."
  (let ((stats (make-hash-table :test 'equal))
        (seen-paths (make-hash-table :test 'equal))
        paths)
    (cl-labels
        ((remember (path)
           (unless (gethash path seen-paths)
             (puthash path t seen-paths)
             (push path paths)))
         (walk (value path doc-seen record-current)
           (when (and path record-current)
             (remember path)
             (clutch-mongodb--profile-record-value
              stats doc-seen path value))
           (cond
            ((and (or (mongodb-document-p value)
                      (clutch-mongodb--alist-p value))
                  (not (clutch-mongodb--extended-json-wrapper-p value)))
             (dolist (pair (clutch-mongodb--document-elements value))
               (walk (cdr pair)
                     (if path
                         (format "%s.%s" path (car pair))
                       (car pair))
                     doc-seen
                     t)))
            ((or (vectorp value)
                 (and (listp value)
                      (not (clutch-mongodb--alist-p value))))
             (dolist (item (if (vectorp value) (append value nil) value))
               (when (or (mongodb-document-p item)
                         (clutch-mongodb--alist-p item))
                 (walk item path doc-seen nil)))))))
      (dolist (doc docs)
        (walk doc nil (make-hash-table :test 'equal) nil)))
    (mapcar
     (lambda (path)
       (let ((stat (gethash path stats)))
         (plist-put stat :values
                    (nreverse (plist-get stat :values)))))
     (nreverse paths))))

(defun clutch-mongodb--profile-field-json (stat sample-size)
  "Return JSON-ready schema profile field object for STAT and SAMPLE-SIZE."
  (let* ((values (plist-get stat :values))
         (present (plist-get stat :present))
         (field `(("path" . ,(plist-get stat :path))
                  ("type" . ,(clutch-mongodb--sample-field-type values))
                  ("typeCategory" . ,(format "%s"
                                             (clutch-mongodb--field-type-category
                                              values)))
                  ("present" . ,present)
                  ("sampled" . ,sample-size)
                  ("presence" . ,(if (> sample-size 0)
                                     (/ (float present) sample-size)
                                   0.0))
                  ("typeCounts" . ,(clutch-mongodb--field-type-counts values)))))
    (when-let* ((examples (plist-get stat :examples)))
      (setq field (append field `(("examples" . ,(vconcat examples))))))
    (when (numberp (plist-get stat :numeric-min))
      (setq field (append field
                          `(("min" . ,(plist-get stat :numeric-min))
                            ("max" . ,(plist-get stat :numeric-max))))))
    (when (> (hash-table-count (plist-get stat :top-values)) 0)
      (setq field (append field
                          `(("topValues" . ,(clutch-mongodb--profile-top-values
                                             stat))))))
    field))

(defun clutch-mongodb--columns-for-docs (keys docs)
  "Return Clutch column plists for KEYS sampled from DOCS."
  (mapcar
   (lambda (key)
     (let ((values (cl-loop for doc in docs
                            for pair = (assoc key doc)
                            when pair collect (cdr pair))))
       (list :name key
             :type-category (clutch-mongodb--field-type-category values))))
   keys))

(defun clutch-mongodb--column-details-for-docs (docs)
  "Return Clutch column-detail plists sampled from MongoDB DOCS."
  (mapcar
   (lambda (stat)
     (let ((values (plist-get stat :values))
           (present (plist-get stat :present))
           (total (length docs)))
       (list :name (plist-get stat :path)
             :type (clutch-mongodb--sample-field-type values)
             :type-category (clutch-mongodb--field-type-category values)
             :nullable t
             :comment (clutch-mongodb--sample-field-comment present total))))
   (clutch-mongodb--profile-stats-for-docs docs)))

(defun clutch-mongodb--result-from-docs (conn docs)
  "Return a Clutch result for CONN from MongoDB document list DOCS."
  (let* ((keys (clutch-mongodb--ordered-keys docs))
         (source-column (list :name clutch-mongodb--source-document-column
                              :type-category 'json
                              :hidden t
                              :document-source t))
         (columns (append (clutch-mongodb--columns-for-docs keys docs)
                          (list source-column)))
         (rows (mapcar
                (lambda (doc)
                  (append
                   (mapcar
                    (lambda (key)
                      (clutch-mongodb--display-value (cdr (assoc key doc))))
                    keys)
                   (list doc)))
                docs)))
    (make-clutch-db-result
     :connection conn
     :columns columns
     :rows rows)))

(defun clutch-mongodb--result-from-value (conn value)
  "Return a Clutch result for CONN from arbitrary MongoDB VALUE."
  (cond
   ((clutch-mongodb--document-list-p value)
    (clutch-mongodb--result-from-docs conn value))
   ((clutch-mongodb--alist-p value)
    (clutch-mongodb--result-from-docs conn (list value)))
   ((and (listp value) (not (clutch-mongodb--alist-p value)))
    (let ((rows (mapcar (lambda (item)
                          (list (clutch-mongodb--display-value item)))
                        value)))
      (make-clutch-db-result
       :connection conn
       :columns (list (list :name "value"
                            :type-category
                            (clutch-mongodb--column-category (car value))))
       :rows rows)))
   (t
    (make-clutch-db-result
     :connection conn
     :columns (list (list :name "value"
                          :type-category
                          (clutch-mongodb--column-category value)))
     :rows (list (list (clutch-mongodb--display-value value)))))))

;;;; Generic methods

(cl-defmethod clutch-db-disconnect ((conn clutch-mongodb-conn))
  "Disconnect MongoDB CONN."
  (setf (clutch-mongodb-conn-closed conn) t)
  (mongodb-disconnect (clutch-mongodb-conn-client conn)))

(cl-defmethod clutch-db-live-p ((conn clutch-mongodb-conn))
  "Return non-nil when MongoDB CONN is still usable."
  (and conn
       (not (clutch-mongodb-conn-closed conn))
       (mongodb-live-p (clutch-mongodb-conn-client conn))))

(cl-defmethod clutch-db-backend-key ((_conn clutch-mongodb-conn))
  "Return the registered backend key for MongoDB connections."
  'mongodb)

(cl-defmethod clutch-db-init-connection ((_conn clutch-mongodb-conn))
  "No eager MongoDB initialization is required.")

(cl-defmethod clutch-db-query ((conn clutch-mongodb-conn) code)
  "Evaluate MongoDB shell CODE on CONN and return a `clutch-db-result'."
  (setf (clutch-mongodb-conn-busy conn) t)
  (unwind-protect
      (clutch-mongodb--result-from-value
       conn
       (clutch-mongodb--eval conn code))
    (setf (clutch-mongodb-conn-busy conn) nil)))

(cl-defmethod clutch-db-result-query-p ((_conn clutch-mongodb-conn) _code)
  "Return non-nil because native MongoDB helpers return result documents."
  t)

(cl-defmethod clutch-db-query-result-context ((_conn clutch-mongodb-conn) code)
  "Return native MongoDB result context for CODE."
  (let* ((call (clutch-mongodb--last-helper-call code))
         (collection (plist-get call :collection)))
    (append
     (list :row-identity-prep (list :sql code)
           :server-pageable nil
           :server-rewritable nil)
     (when collection
       (list :source-table collection)))))

(cl-defmethod clutch-db-build-paged-sql ((_conn clutch-mongodb-conn)
                                         base-code page-num page-size
                                         &optional _order-by page-offset)
  "Return BASE-CODE unchanged for CONN, PAGE-NUM, PAGE-SIZE, and PAGE-OFFSET.
Native MongoDB scripts are not SQL and cannot be safely paginated by appending
SQL clauses.  Use cursor methods such as `.skip(N).limit(M)' in the query."
  (ignore page-num page-size page-offset)
  base-code)

(cl-defmethod clutch-db-escape-identifier ((_conn clutch-mongodb-conn) name)
  "Return NAME unchanged; native MongoDB has no SQL identifier syntax."
  name)

(cl-defmethod clutch-db-escape-literal ((_conn clutch-mongodb-conn) value)
  "Return VALUE as a JSON literal for MongoDB Shell snippets."
  (clutch-mongodb--json-encode-text value))

(cl-defmethod clutch-db-list-schemas ((conn clutch-mongodb-conn))
  "Return database names visible to MongoDB CONN."
  (list (clutch-mongodb-conn-database conn)))

(cl-defmethod clutch-db-current-schema ((conn clutch-mongodb-conn))
  "Return the current MongoDB database name for CONN."
  (clutch-mongodb-conn-database conn))

(cl-defmethod clutch-db-set-current-schema ((conn clutch-mongodb-conn) schema)
  "Switch MongoDB CONN to SCHEMA for subsequent mongodb.el commands."
  (setf (clutch-mongodb-conn-database conn) schema)
  (setf (clutch-mongodb-conn-params conn)
        (plist-put (copy-sequence (clutch-mongodb-conn-params conn))
                   :database schema))
  schema)

(cl-defmethod clutch-db-list-tables ((conn clutch-mongodb-conn))
  "Return collection names for CONN's current MongoDB database."
  (clutch-mongodb--with-mongodb-errors
    (mongodb-list-collections
     (clutch-mongodb-conn-client conn)
     (clutch-mongodb-conn-database conn))))

(cl-defmethod clutch-db-list-table-entries ((conn clutch-mongodb-conn))
  "Return collection entries for MongoDB CONN."
  (mapcar (lambda (name)
            (list :name name
                  :schema (clutch-mongodb-conn-database conn)
                  :type "COLLECTION"))
          (clutch-db-list-tables conn)))

(cl-defmethod clutch-db-search-table-entries ((conn clutch-mongodb-conn) prefix)
  "Return MongoDB collection entries for CONN matching PREFIX."
  (seq-filter
   (lambda (entry)
     (string-prefix-p
      (downcase prefix)
      (downcase (plist-get entry :name))))
   (clutch-db-list-table-entries conn)))

(cl-defmethod clutch-db-complete-tables ((conn clutch-mongodb-conn) prefix)
  "Return MongoDB collection names for CONN matching PREFIX."
  (mapcar (lambda (entry) (plist-get entry :name))
          (clutch-db-search-table-entries conn prefix)))

(cl-defmethod clutch-db-list-columns ((conn clutch-mongodb-conn) collection)
  "Return sampled document field paths for COLLECTION on CONN."
  (mapcar (lambda (stat) (plist-get stat :path))
          (clutch-mongodb--profile-stats-for-docs
           (clutch-mongodb--sample-documents conn collection))))

(cl-defmethod clutch-db-column-details ((conn clutch-mongodb-conn) collection)
  "Return sampled column details for MongoDB COLLECTION on CONN."
  (clutch-mongodb--column-details-for-docs
   (clutch-mongodb--sample-documents conn collection)))

(defun clutch-mongodb--collection-info (conn collection)
  "Return the collection info document for COLLECTION on CONN."
  (clutch-mongodb--with-mongodb-errors
    (let ((infos (mongodb-list-collection-docs
                  (clutch-mongodb-conn-client conn)
                  (clutch-mongodb-conn-database conn)
                  (mongodb-document (list (cons "name" collection))))))
      (unless (listp infos)
        (signal 'clutch-db-error
                (list "MongoDB collection metadata returned a non-list result")))
      (or (car infos)
          (signal 'clutch-db-error
                  (list (format "MongoDB collection not found: %s" collection)))))))

(cl-defmethod clutch-db-object-browse-query
  ((_conn clutch-mongodb-conn) entry)
  "Return MongoDB helper syntax to browse collection ENTRY."
  (format "db.getCollection(%s).find({}).limit(20);"
          (clutch--json-serialize-text
           (plist-get entry :name)
           "MongoDB collection name")))

(defun clutch-mongodb--collection-validation (conn collection)
  "Return collection validation metadata for MongoDB COLLECTION on CONN."
  (let* ((info (clutch-mongodb--collection-info conn collection))
         (options (clutch-mongodb--document-value info "options"))
         (validator (and options
                         (clutch-mongodb--document-value options "validator")))
         (validation-action
          (and options
               (clutch-mongodb--document-value options "validationAction")))
         (validation-level
          (and options
               (clutch-mongodb--document-value options "validationLevel"))))
    (clutch-mongodb--json-encode-text
     `(("collection" . ,collection)
       ("configured" . ,(if validator t :false))
       ("validationAction" . ,validation-action)
	   ("validationLevel" . ,validation-level)
	   ("validator" . ,validator)))))

(defun clutch-mongodb--collection-stats (conn collection)
  "Return collection storage statistics for MongoDB COLLECTION on CONN."
  (let* ((storage-stage
          (mongodb-document
           (list (cons "storageStats" (mongodb-document nil)))))
         (pipeline
          (vector
           (mongodb-document
            (list (cons "$collStats" storage-stage)))))
         (docs
          (mongodb-aggregate
           (clutch-mongodb-conn-client conn)
           (clutch-mongodb-conn-database conn)
           collection
           pipeline))
         (stats (and (listp docs) (car docs)))
         (storage-stats
          (and stats
               (clutch-mongodb--document-value stats "storageStats"))))
    (unless (listp docs)
      (signal 'clutch-db-error
              (list "MongoDB collection stats returned a non-list result")))
    (unless stats
      (signal 'clutch-db-error
              (list (format "MongoDB collection stats returned no data: %s"
                            collection))))
    (unless storage-stats
      (signal 'clutch-db-error
              (list (format "MongoDB collection stats missing storageStats: %s"
                            collection))))
    (clutch-mongodb--json-encode-text
     `(("collection" . ,collection)
       ("namespace" . ,(clutch-mongodb--document-value stats "ns"))
       ("count" . ,(clutch-mongodb--document-value storage-stats "count"))
       ("size" . ,(clutch-mongodb--document-value storage-stats "size"))
       ("avgObjSize" . ,(clutch-mongodb--document-value
                         storage-stats "avgObjSize"))
       ("storageSize" . ,(clutch-mongodb--document-value
                          storage-stats "storageSize"))
       ("nindexes" . ,(clutch-mongodb--document-value
                       storage-stats "nindexes"))
       ("totalIndexSize" . ,(clutch-mongodb--document-value
                             storage-stats "totalIndexSize"))
       ("totalSize" . ,(clutch-mongodb--document-value
                        storage-stats "totalSize"))
       ("indexSizes" . ,(clutch-mongodb--document-value
                         storage-stats "indexSizes"))))))

(defun clutch-mongodb--collection-index-documents (conn collection)
  "Return raw MongoDB index documents for COLLECTION on CONN."
  (mongodb-list-indexes
   (clutch-mongodb-conn-client conn)
   (clutch-mongodb-conn-database conn)
   collection))

(defun clutch-mongodb--collection-index-stats (conn collection)
  "Return raw MongoDB `$indexStats' documents for COLLECTION on CONN."
  (let ((pipeline
         (vector
          (mongodb-document
           (list (cons "$indexStats" (mongodb-document nil)))))))
    (mongodb-aggregate
     (clutch-mongodb-conn-client conn)
     (clutch-mongodb-conn-database conn)
     collection
     pipeline)))

(defun clutch-mongodb--index-stats-by-name (stats-docs)
  "Return a hash table mapping index names to STATS-DOCS entries."
  (let ((table (make-hash-table :test 'equal)))
    (dolist (doc stats-docs)
      (when-let* ((name (clutch-mongodb--document-value doc "name")))
        (puthash name doc table)))
    table))

(cl-defmethod clutch-db-collection-profile ((conn clutch-mongodb-conn) collection)
  "Return MongoDB schema profile metadata for COLLECTION on CONN as JSON."
  (clutch-mongodb--with-mongodb-errors
    (let* ((docs (clutch-mongodb--sample-documents conn collection))
           (sample-size (length docs))
           (stats (clutch-mongodb--profile-stats-for-docs docs))
           (indexes (clutch-mongodb--collection-index-documents conn collection)))
      (clutch-mongodb--json-encode-text
       `(("collection" . ,collection)
         ("database" . ,(clutch-mongodb-conn-database conn))
         ("sampleSize" . ,sample-size)
         ("sampleLimit" . ,(clutch-mongodb--schema-sample-limit))
         ("fields" . ,(vconcat
                        (mapcar
                         (lambda (stat)
                           (clutch-mongodb--profile-field-json
                            stat sample-size))
                         stats)))
         ("indexes" . ,(vconcat
                         (mapcar #'clutch-mongodb--index-json indexes))))))))

(defun clutch-mongodb--collection-index-insight (conn collection)
  "Return MongoDB index insight for COLLECTION on CONN.
The returned text is JSON metadata."
  (let* ((index-docs (clutch-mongodb--collection-index-documents
                      conn collection))
         (stats-docs (clutch-mongodb--collection-index-stats
                      conn collection))
         (stats-by-name (clutch-mongodb--index-stats-by-name stats-docs)))
    (clutch-mongodb--json-encode-text
     `(("collection" . ,collection)
       ("database" . ,(clutch-mongodb-conn-database conn))
       ("indexes" . ,(vconcat
                       (mapcar
                        (lambda (document)
                          (clutch-mongodb--index-json
                           document
                           (gethash
                            (clutch-mongodb--document-value document "name")
                            stats-by-name)))
                        index-docs)))))))

(cl-defmethod clutch-db-object-action-supported-p
  ((_conn clutch-mongodb-conn) entry action-id)
  "Return non-nil when ACTION-ID is a MongoDB collection action for ENTRY."
  (and (string= (upcase (or (plist-get entry :type) "")) "COLLECTION")
       (memq action-id
             '(index-insight explain-sample show-validation show-stats))))

(cl-defmethod clutch-db-object-action-metadata
  ((conn clutch-mongodb-conn) entry action-id)
  "Return MongoDB metadata text for collection ACTION-ID on ENTRY using CONN."
  (clutch-mongodb--with-mongodb-errors
    (when (clutch-db-object-action-supported-p conn entry action-id)
      (let ((collection (plist-get entry :name)))
        (pcase action-id
          ('index-insight
           (clutch-mongodb--collection-index-insight conn collection))
          ('explain-sample
           (clutch-mongodb--collection-explain-sample conn collection))
          ('show-validation
           (clutch-mongodb--collection-validation conn collection))
          ('show-stats
           (clutch-mongodb--collection-stats conn collection)))))))

(cl-defmethod clutch-db-document-mutation-supported-p
  ((_conn clutch-mongodb-conn) action)
  "Return non-nil when ACTION has a native MongoDB helper snippet."
  (memq action '(insert-one insert-many replace-one delete-one update-one-set)))

(cl-defmethod clutch-db-document-mutation-snippets
  ((_conn clutch-mongodb-conn) action collection documents &optional fields)
  "Return native MongoDB helper snippets for ACTION on COLLECTION.
DOCUMENTS is a list of original MongoDB documents.  FIELDS is an optional list
of top-level field names for field-scoped snippets."
  (clutch-mongodb--document-mutation-snippet
   action collection documents fields))

(defun clutch-mongodb--collection-explain-sample (conn collection)
  "Return MongoDB explain metadata for a sample query on COLLECTION using CONN."
  (clutch-db-explain-query
   conn
   (format "db.getCollection(%s).find({}).limit(1);"
           (clutch-mongodb--json-encode-text collection))))

(cl-defmethod clutch-db-explain-query ((conn clutch-mongodb-conn) query)
  "Return MongoDB explain metadata for QUERY on CONN as JSON."
  (let* ((call (clutch-mongodb--single-helper-call query "explain"))
         (explain (clutch-mongodb--with-mongodb-errors
                    (clutch-mongodb--explain-call conn call))))
    (clutch-mongodb--json-encode-text
     `(("summary" . ,(clutch-mongodb--explain-summary explain))
       ("explain" . ,(clutch-mongodb--json-encodable explain))))))

(cl-defmethod clutch-db-list-objects ((conn clutch-mongodb-conn) category)
  "Return MongoDB object entries in CATEGORY for CONN."
  (clutch-mongodb--with-mongodb-errors
    (pcase category
      ('indexes
       (cl-loop for collection in (clutch-db-list-tables conn)
                append
                (mapcar (lambda (document)
                          (clutch-mongodb--index-entry conn collection document))
                        (clutch-mongodb--collection-index-documents
                         conn collection))))
      (_ nil))))

(cl-defmethod clutch-db-object-details ((conn clutch-mongodb-conn) entry)
  "Return MongoDB object details for ENTRY on CONN."
  (clutch-mongodb--with-mongodb-errors
    (pcase (upcase (or (plist-get entry :type) ""))
      ("INDEX"
       (when-let* ((document (clutch-mongodb--index-document conn entry))
                   (key (clutch-mongodb--document-value document "key")))
         (clutch-mongodb--index-key-details key))))))

(cl-defmethod clutch-db-object-definition ((conn clutch-mongodb-conn) entry)
  "Return MongoDB object metadata for ENTRY on CONN as JSON."
  (clutch-mongodb--with-mongodb-errors
    (pcase (upcase (or (plist-get entry :type) ""))
      ("COLLECTION"
       (clutch-mongodb--json-encode-text
        (list (clutch-mongodb--collection-info
               conn (plist-get entry :name)))))
      ("INDEX"
       (when-let* ((document (clutch-mongodb--index-document conn entry)))
         (clutch-mongodb--json-encode-text document))))))

(cl-defmethod clutch-db-table-comment ((_conn clutch-mongodb-conn) _table
                                       &optional _schema)
  "Return nil; MongoDB collections have no SQL table comments."
  nil)

(cl-defmethod clutch-db-primary-key-columns ((_conn clutch-mongodb-conn) _table)
  "Return nil; native MongoDB results are not edited through SQL row identity."
  nil)

(cl-defmethod clutch-db-row-identity-candidates ((_conn clutch-mongodb-conn) _table
                                                 &optional _schema _catalog)
  "Return nil; native MongoDB staged SQL edits are unsupported."
  nil)

(cl-defmethod clutch-db-foreign-keys ((_conn clutch-mongodb-conn) _table)
  "Return nil; MongoDB has no SQL foreign-key metadata."
  nil)

(cl-defmethod clutch-db-referencing-objects ((_conn clutch-mongodb-conn) _table)
  "Return nil; MongoDB has no SQL referencing-object metadata."
  nil)

(cl-defmethod clutch-db-busy-p ((conn clutch-mongodb-conn))
  "Return non-nil when MongoDB CONN is executing a mongodb.el command."
  (clutch-mongodb-conn-busy conn))

(cl-defmethod clutch-db-user ((conn clutch-mongodb-conn))
  "Return the effective MongoDB user for CONN, if any."
  (mongodb-connection-username (clutch-mongodb-conn-client conn)))

(cl-defmethod clutch-db-host ((conn clutch-mongodb-conn))
  "Return the effective MongoDB host for CONN, if any."
  (mongodb-connection-host (clutch-mongodb-conn-client conn)))

(cl-defmethod clutch-db-port ((conn clutch-mongodb-conn))
  "Return the effective MongoDB port for CONN, if any."
  (mongodb-connection-port (clutch-mongodb-conn-client conn)))

(cl-defmethod clutch-db-database ((conn clutch-mongodb-conn))
  "Return the current MongoDB database for CONN."
  (clutch-mongodb-conn-database conn))

(cl-defmethod clutch-db-display-name ((_conn clutch-mongodb-conn))
  "Return \"MongoDB\" as the display name."
  "MongoDB")

(provide 'clutch-mongodb)
;;; clutch-mongodb.el ends here
