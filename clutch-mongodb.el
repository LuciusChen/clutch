;;; clutch-mongodb.el --- Native MongoDB backend adapter -*- lexical-binding: t; -*-

;; Copyright (C) 2025-2026 Lucius Chen
;; SPDX-License-Identifier: GPL-3.0-or-later

;; Author: Lucius Chen <chenyh572@gmail.com>
;; Maintainer: Lucius Chen <chenyh572@gmail.com>
;; Version: 0.1.0
;; Keywords: data, tools
;; URL: https://github.com/LuciusChen/clutch

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
(declare-function mongodb-aggregate-database "mongodb" (client database pipeline &optional options))
(declare-function mongodb-command "mongodb" (client database command &optional timeout sequences))
(declare-function mongodb-connect "mongodb" (params))
(declare-function mongodb-count-documents "mongodb" (client database collection filter &optional options))
(declare-function mongodb-create-collection "mongodb" (client database collection &optional options))
(declare-function mongodb-create-index "mongodb" (client database collection keys &optional options))
(declare-function mongodb-decimal128 "mongodb-bson" (value))
(declare-function mongodb-delete "mongodb" (client database collection filter &optional multi))
(declare-function mongodb-datetime "mongodb-bson" (millis))
(declare-function mongodb-disconnect "mongodb" (conn))
(declare-function mongodb-distinct "mongodb" (client database collection field-name &optional query options))
(declare-function mongodb-document "mongodb-bson" (elements))
(declare-function mongodb-document-elements "mongodb-bson" (document))
(declare-function mongodb-document-p "mongodb-bson" (value))
(declare-function mongodb-drop-collection "mongodb" (client database collection))
(declare-function mongodb-drop-database "mongodb" (client database))
(declare-function mongodb-drop-index "mongodb" (client database collection index))
(declare-function mongodb-explain "mongodb" (client database command &optional verbosity))
(declare-function mongodb-find "mongodb" (client database collection filter &optional projection limit skip sort options))
(declare-function mongodb-find-command "mongodb" (collection filter &optional projection limit skip sort options))
(declare-function mongodb-insert "mongodb" (client database collection documents))
(declare-function mongodb-int32 "mongodb-bson" (value))
(declare-function mongodb-int64 "mongodb-bson" (value))
(declare-function mongodb-list-collection-docs "mongodb" (client database &optional filter options))
(declare-function mongodb-list-collections "mongodb" (client database &optional filter options))
(declare-function mongodb-list-indexes "mongodb" (client database collection))
(declare-function mongodb-live-p "mongodb" (conn))
(declare-function mongodb-object-id "mongodb-bson" (hex))
(declare-function mongodb-timestamp "mongodb-bson" (time increment))
(declare-function mongodb-update "mongodb" (client database collection filter update &optional multi upsert))
(declare-function mongodb-conn-database "mongodb" (conn))

;;;; Configuration

(defmacro clutch-mongodb--with-mongodb-errors (&rest body)
  "Run BODY and translate `mongodb-error' to `clutch-db-error'."
  (declare (indent 0) (debug t))
  `(condition-case err
       (progn ,@body)
     (mongodb-error
      (signal 'clutch-db-error (cdr err)))))

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

(defun clutch-mongodb--sql-interface-surface-p (params)
  "Return non-nil when PARAMS select MongoDB SQL Interface."
  (memq (clutch-mongodb--surface params) '(sql sql-interface)))

(defun clutch-mongodb--validate-surface (params)
  "Signal if PARAMS contain an unsupported MongoDB surface."
  (pcase (clutch-mongodb--surface params)
    ((or 'nil 'sql 'sql-interface) nil)
    (surface
     (signal 'clutch-db-error
             (list (format "Unsupported MongoDB :surface %S" surface))))))

(defconst clutch-mongodb--required-mongodb-functions
  '(mongodb-aggregate
    mongodb-aggregate-command
    mongodb-aggregate-database
    mongodb-command
    mongodb-connect
    mongodb-count-documents
    mongodb-create-collection
    mongodb-create-index
    mongodb-decimal128
    mongodb-delete
    mongodb-datetime
    mongodb-disconnect
    mongodb-distinct
    mongodb-document
    mongodb-document-elements
    mongodb-document-p
    mongodb-drop-collection
    mongodb-drop-database
    mongodb-drop-index
    mongodb-explain
    mongodb-find
    mongodb-find-command
    mongodb-insert
    mongodb-int32
    mongodb-int64
    mongodb-list-collection-docs
    mongodb-list-collections
    mongodb-list-indexes
    mongodb-live-p
    mongodb-object-id
    mongodb-timestamp
    mongodb-update
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
  (when (clutch-mongodb--missing-mongodb-functions)
    (when-let* ((library (locate-library "mongodb")))
      (condition-case nil
          (load library nil 'nomessage)
        (error nil))))
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
  (if (clutch-mongodb--sql-interface-surface-p params)
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
      (if (string-match-p "[.eE]" token)
          (string-to-number token)
        (string-to-number token)))))

(defun clutch-mongodb--mql-integer-arg (value constructor)
  "Return VALUE as an integer argument for MongoDB CONSTRUCTOR."
  (cond
   ((integerp value) value)
   ((and (stringp value)
         (string-match-p "\\`[-+]?[0-9]+\\'" value))
    (string-to-number value))
   (t
    (signal 'clutch-db-error
            (list (format "%s() expects an integer or integer string"
                          constructor))))))

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
      ("Timestamp"
       (unless (and (= (length args) 2)
                    (integerp (car args))
                    (integerp (cadr args)))
         (signal 'clutch-db-error
                 (list "Timestamp() expects integer seconds and increment")))
       (mongodb-timestamp (car args) (cadr args)))
      ((or "Int32" "NumberInt")
       (unless (= (length args) 1)
         (signal 'clutch-db-error
                 (list (format "%s() expects one integer argument"
                               name))))
       (mongodb-int32
        (clutch-mongodb--mql-integer-arg (car args) name)))
      ((or "Long" "NumberLong")
       (unless (= (length args) 1)
         (signal 'clutch-db-error
                 (list (format "%s() expects one integer argument"
                               name))))
       (mongodb-int64
        (clutch-mongodb--mql-integer-arg (car args) name)))
      ((or "Decimal128" "NumberDecimal")
       (unless (and (= (length args) 1)
                    (stringp (car args)))
         (signal 'clutch-db-error
                 (list (format "%s() expects one decimal string"
                               name))))
       (mongodb-decimal128 (car args)))
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

(defun clutch-mongodb--parse-method-call (text pos collection &optional database)
  "Parse a collection method call in TEXT at POS for COLLECTION.
When DATABASE is non-nil, attach it as the target database override."
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
                 (substring text (1+ close))))
    (append (list :collection collection :method method :args args
                  :chain chain)
            (when database
              (list :database database)))))

(defun clutch-mongodb--parse-helper-chain (text)
  "Parse supported collection helper chain suffix TEXT."
  (let ((tail (string-trim text))
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
            (pcase method
              ((or "limit" "skip")
               (unless (and (= (length args) 1)
                            (integerp (car args)))
                 (signal 'clutch-db-error
                         (list (format "%s() expects one integer argument"
                                       method))))
               (push (cons method (car args)) chain))
              ((or "maxTimeMS" "batchSize")
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
              ("comment"
               (unless (= (length args) 1)
                 (signal 'clutch-db-error
                         (list "comment() expects one argument")))
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

(defun clutch-mongodb--find-options-from-chain (chain)
  "Return MongoDB find command options parsed from helper CHAIN."
  (delq nil
        (mapcar
         (lambda (pair)
           (let ((method (car pair))
                 (value (cdr pair)))
             (when (member method '("sort" "maxTimeMS" "batchSize"
                                    "allowDiskUse" "comment"))
               (cons method value))))
         chain)))

(defun clutch-mongodb--aggregate-options-from-chain (chain)
  "Return MongoDB aggregate command options parsed from helper CHAIN."
  (delq nil
        (mapcar
         (lambda (pair)
           (let ((method (car pair))
                 (value (cdr pair)))
             (when (member method '("allowDiskUse" "batchSize" "comment"
                                    "maxTimeMS"))
               (cons method value))))
         chain)))

(defun clutch-mongodb--merge-option-pairs (base extra)
  "Return a MongoDB option document from BASE pairs with EXTRA overriding."
  (let ((pairs (append
                (cl-remove-if (lambda (pair)
                                (assoc (car pair) extra))
                              (or base nil))
                extra)))
    (and pairs
         (mongodb-document pairs))))

(defun clutch-mongodb--merge-options (base extra)
  "Return option document merged from BASE document and EXTRA alist."
  (clutch-mongodb--merge-option-pairs
   (and base (mongodb-document-elements base))
   extra))

(defun clutch-mongodb--explain-verbosity (chain)
  "Return explain verbosity requested by helper CHAIN, or nil."
  (cdr (assoc "explain" chain)))

(defun clutch-mongodb--parse-db-member-call (text rest-pos &optional database)
  "Parse a MongoDB DB member call in TEXT starting at REST-POS.
DATABASE, when non-nil, is the database targeted by the parsed helper."
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
      (clutch-mongodb--parse-method-call
       text (1+ close) (car args) database)))
   ((string-prefix-p "getSiblingDB(" (substring text rest-pos))
    (let* ((open (+ rest-pos (length "getSiblingDB")))
           (parsed (clutch-mongodb--parse-call-args text open))
           (args (car parsed))
           (close (cdr parsed)))
      (unless (and (= (length args) 1)
                   (stringp (car args)))
        (signal 'clutch-db-error
                (list "db.getSiblingDB() expects one database name string")))
      (let ((tail (string-trim (substring text (1+ close)))))
        (if (string-empty-p tail)
            (list :db-method "getSiblingDB" :args args)
          (unless (string-prefix-p "." tail)
            (signal 'clutch-db-error
                    (list "Unsupported MongoDB getSiblingDB() helper chain")))
          (clutch-mongodb--parse-db-member-call
           tail 1 (car args))))))
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
            (append (list :db-method token :args args)
                    (when database
                      (list :database database))))
        (clutch-mongodb--parse-method-call
         text token-end token database))))))

(defun clutch-mongodb--parse-db-call (statement)
  "Parse one MongoDB shell helper STATEMENT."
  (let ((text (string-trim statement)))
    (unless (string-prefix-p "db." text)
      (signal 'clutch-db-error
              (list "Native MongoDB supports db.* helper calls, not arbitrary JavaScript")))
    (clutch-mongodb--parse-db-member-call text 3)))

(defun clutch-mongodb--mql-doc-or-empty (value)
  "Return VALUE or an empty MongoDB document."
  (or value (mongodb-document nil)))

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

(defun clutch-mongodb--execute-db-method
    (conn method args &optional target-database)
  "Execute database-level METHOD with ARGS on CONN.
TARGET-DATABASE, when non-nil, overrides CONN's current database for this
helper call."
  (let ((client (clutch-mongodb-conn-client conn))
        (database (or target-database
                      (clutch-mongodb-conn-database conn))))
    (pcase method
      ("getName"
       (unless (null args)
         (signal 'clutch-db-error
                 (list "db.getName() does not accept arguments")))
       database)
      ("getSiblingDB"
       (unless (and (= (length args) 1)
                    (stringp (car args)))
         (signal 'clutch-db-error
                 (list "db.getSiblingDB() expects one database name string")))
       (car args))
      ("aggregate"
       (unless (and (<= 1 (length args) 2)
                    (vectorp (car args)))
         (signal 'clutch-db-error
                 (list "db.aggregate() expects a pipeline array and optional options document")))
       (when (and (nth 1 args)
                  (not (mongodb-document-p (nth 1 args))))
         (signal 'clutch-db-error
                 (list "db.aggregate() options argument must be a document")))
       (mongodb-aggregate-database
        client database (car args) (nth 1 args)))
      ("adminCommand"
       (unless (= (length args) 1)
         (signal 'clutch-db-error
                 (list "db.adminCommand() expects one command document")))
       (mongodb-command client "admin" (car args)))
      ("runCommand"
       (unless (= (length args) 1)
         (signal 'clutch-db-error
                 (list "db.runCommand() expects one command document")))
       (mongodb-command client database (car args)))
      ("getCollectionNames"
       (unless (null args)
         (signal 'clutch-db-error
                 (list "db.getCollectionNames() does not accept arguments")))
       (mongodb-list-collections client database))
      ("getCollectionInfos"
       (mongodb-list-collection-docs
        client database (car args)))
      ("createCollection"
       (unless (and (<= 1 (length args) 2)
                    (stringp (car args)))
         (signal 'clutch-db-error
                 (list "db.createCollection() expects collection name and optional options document")))
       (mongodb-create-collection
        client database
        (car args)
        (clutch-mongodb--mql-optional-document-arg
         (nth 1 args) method 2)))
      ("dropDatabase"
       (unless (null args)
         (signal 'clutch-db-error
                 (list "db.dropDatabase() does not accept arguments")))
       (mongodb-drop-database client database))
      (_
       (signal 'clutch-db-error
               (list (format "Unsupported MongoDB db helper: %s" method)))))))

(defun clutch-mongodb--execute-collection-method
    (conn collection method args &optional chain target-database)
  "Execute collection METHOD on COLLECTION with ARGS on CONN.
CHAIN contains parsed cursor helper calls, when present.
TARGET-DATABASE, when non-nil, overrides CONN's current database for this
helper call."
  (let ((client (clutch-mongodb-conn-client conn))
        (database (or target-database
                      (clutch-mongodb-conn-database conn))))
    (pcase method
      ("find"
       (let ((filter (clutch-mongodb--mql-doc-or-empty (nth 0 args)))
             (projection (nth 1 args))
             (limit (cdr (assoc "limit" chain)))
             (skip (cdr (assoc "skip" chain)))
             (options (clutch-mongodb--find-options-from-chain chain)))
         (if (assoc "explain" chain)
             (mongodb-explain
              client database
              (mongodb-find-command collection filter projection limit skip
                                  options)
              (clutch-mongodb--explain-verbosity chain))
           (mongodb-find
            client database collection filter projection limit skip options))))
      ("findOne"
       (car (mongodb-find
             client database collection
             (clutch-mongodb--mql-doc-or-empty (nth 0 args))
             (nth 1 args)
             1)))
      ("countDocuments"
       (unless (<= (length args) 2)
         (signal 'clutch-db-error
                 (list "countDocuments() expects optional filter and options")))
       (mongodb-count-documents
        client database collection
        (clutch-mongodb--mql-doc-or-empty (nth 0 args))
        (clutch-mongodb--mql-optional-document-arg
         (nth 1 args) method 2)))
      ("estimatedDocumentCount"
       (unless (<= (length args) 1)
         (signal 'clutch-db-error
                 (list "estimatedDocumentCount() expects optional options")))
       (mongodb-count-documents
        client database collection nil
        (clutch-mongodb--mql-optional-document-arg
         (nth 0 args) method 1)))
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
       (unless (and (<= 1 (length args) 2)
                    (vectorp (car args)))
         (signal 'clutch-db-error
                 (list "aggregate() expects a pipeline array and optional options document")))
       (when (and (nth 1 args)
                  (not (mongodb-document-p (nth 1 args))))
         (signal 'clutch-db-error
                 (list "aggregate() options argument must be a document")))
       (let ((options (clutch-mongodb--merge-options
                       (nth 1 args)
                       (clutch-mongodb--aggregate-options-from-chain
                        chain))))
         (if (assoc "explain" chain)
             (mongodb-explain
              client database
              (mongodb-aggregate-command collection (car args) options)
              (clutch-mongodb--explain-verbosity chain))
           (mongodb-aggregate
            client database collection (car args) options))))
      ("listIndexes"
       (unless (null args)
         (signal 'clutch-db-error
                 (list "listIndexes() does not accept arguments")))
       (mongodb-list-indexes client database collection))
      ("createIndex"
       (unless (<= 1 (length args) 2)
         (signal 'clutch-db-error
                 (list "createIndex() expects keys and optional options")))
       (mongodb-create-index
        client database collection
        (clutch-mongodb--mql-document-arg (nth 0 args) method 1)
        (clutch-mongodb--mql-optional-document-arg
         (nth 1 args) method 2)))
      ("dropIndex"
       (unless (= (length args) 1)
         (signal 'clutch-db-error
                 (list "dropIndex() expects one index name or key document")))
       (mongodb-drop-index client database collection (car args)))
      ("insertOne"
       (unless (= (length args) 1)
         (signal 'clutch-db-error
                 (list "insertOne() expects one document")))
       (mongodb-insert client database collection (car args)))
      ("insertMany"
       (unless (= (length args) 1)
         (signal 'clutch-db-error
                 (list "insertMany() expects one document array")))
       (mongodb-insert client database collection (car args)))
      ("deleteMany"
       (mongodb-delete
        client database collection
        (clutch-mongodb--mql-doc-or-empty (car args))
        0))
      ("deleteOne"
       (mongodb-delete
        client database collection
        (clutch-mongodb--mql-doc-or-empty (car args))
        1))
      ("updateOne"
       (unless (<= 2 (length args) 3)
         (signal 'clutch-db-error
                 (list "updateOne() expects filter, update, and optional options")))
       (mongodb-update
        client database collection
        (clutch-mongodb--mql-document-arg (nth 0 args) method 1)
        (nth 1 args)
        nil
        (clutch-mongodb--mql-optional-document-arg
         (nth 2 args) method 3)))
      ("updateMany"
       (unless (<= 2 (length args) 3)
         (signal 'clutch-db-error
                 (list "updateMany() expects filter, update, and optional options")))
       (mongodb-update
        client database collection
        (clutch-mongodb--mql-document-arg (nth 0 args) method 1)
        (nth 1 args)
        t
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
      ("drop"
       (unless (null args)
         (signal 'clutch-db-error
                 (list "drop() does not accept arguments")))
       (mongodb-drop-collection client database collection))
      (_
       (signal 'clutch-db-error
               (list (format "Unsupported MongoDB collection helper: %s"
                             method)))))))

(defun clutch-mongodb--eval-one (conn statement)
  "Evaluate one parsed MongoDB helper STATEMENT on CONN."
  (let ((call (clutch-mongodb--parse-db-call statement)))
    (pcase call
      (`(:db-method ,method :args ,args :database ,database)
       (clutch-mongodb--execute-db-method conn method args database))
      (`(:db-method ,method :args ,args)
       (clutch-mongodb--execute-db-method conn method args))
      (`(:collection ,collection :method ,method :args ,args :chain ,chain
                     :database ,database)
       (clutch-mongodb--execute-collection-method
        conn collection method args chain database))
      (`(:collection ,collection :method ,method :args ,args :chain ,chain)
       (clutch-mongodb--execute-collection-method
        conn collection method args chain)))))

(defun clutch-mongodb--eval (conn code)
  "Evaluate supported MongoDB shell helper CODE on CONN."
  (clutch-mongodb--with-mongodb-errors
    (let (value)
      (dolist (statement (clutch-mongodb--split-statements code))
        (setq value (clutch-mongodb--eval-one conn statement)))
      value)))

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
   ((clutch-mongodb--alist-p value)
    (mapcar (lambda (pair)
              (cons (car pair)
                    (clutch-mongodb--json-encodable (cdr pair))))
            value))
   ((listp value)
    (vconcat (mapcar #'clutch-mongodb--json-encodable value)))
   (t value)))

(defun clutch-mongodb--json-encode-text (value)
  "Return VALUE encoded as JSON text for MongoDB result display."
  (json-encode (clutch-mongodb--json-encodable value)))

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

(defun clutch-mongodb--columns-for-docs (keys docs)
  "Return Clutch column plists for KEYS sampled from DOCS."
  (mapcar
   (lambda (key)
     (let ((sample (cl-loop for doc in docs
                            for value = (cdr (assoc key doc))
                            when value return value)))
       (list :name key
             :type-category (clutch-mongodb--column-category sample))))
   keys))

(defun clutch-mongodb--result-from-docs (conn docs)
  "Return a Clutch result for CONN from MongoDB document list DOCS."
  (let* ((keys (clutch-mongodb--ordered-keys docs))
         (columns (clutch-mongodb--columns-for-docs keys docs))
         (rows (mapcar
                (lambda (doc)
                  (mapcar
                   (lambda (key)
                     (clutch-mongodb--display-value (cdr (assoc key doc))))
                   keys))
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
  (clutch-mongodb--eval conn "db.getCollectionNames()"))

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
  "Return sampled top-level document keys for COLLECTION on CONN."
  (let* ((code (format "db.getCollection(%s).findOne()"
                       (clutch--json-serialize-text collection
                                                    "MongoDB collection name")))
         (doc (clutch-mongodb--eval conn code)))
    (if (clutch-mongodb--alist-p doc)
        (clutch-mongodb--ordered-keys (list doc))
      nil)))

(cl-defmethod clutch-db-column-details ((conn clutch-mongodb-conn) collection)
  "Return sampled column details for MongoDB COLLECTION on CONN."
  (mapcar (lambda (name)
            (list :name name
                  :type "BSON"
                  :type-category 'json
                  :nullable t))
          (clutch-db-list-columns conn collection)))

(cl-defmethod clutch-db-show-create-table ((conn clutch-mongodb-conn) collection)
  "Return collection metadata for COLLECTION on CONN as JSON."
  (let ((info (clutch-mongodb--eval
              conn
              (format "db.getCollectionInfos({name: %s})"
                      (clutch--json-serialize-text collection
                                                   "MongoDB collection name")))))
    (clutch-mongodb--json-encode-text info)))

(cl-defmethod clutch-db-table-comment ((_conn clutch-mongodb-conn) _table)
  "Return nil; MongoDB collections have no SQL table comments."
  nil)

(cl-defmethod clutch-db-primary-key-columns ((_conn clutch-mongodb-conn) _table)
  "Return nil; native MongoDB results are not edited through SQL row identity."
  nil)

(cl-defmethod clutch-db-row-identity-candidates ((_conn clutch-mongodb-conn) _table)
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
  "Return the configured MongoDB user for CONN, if any."
  (plist-get (clutch-mongodb-conn-params conn) :user))

(cl-defmethod clutch-db-host ((conn clutch-mongodb-conn))
  "Return the configured MongoDB host for CONN, if any."
  (plist-get (clutch-mongodb-conn-params conn) :host))

(cl-defmethod clutch-db-port ((conn clutch-mongodb-conn))
  "Return the configured MongoDB port for CONN, if any."
  (plist-get (clutch-mongodb-conn-params conn) :port))

(cl-defmethod clutch-db-database ((conn clutch-mongodb-conn))
  "Return the current MongoDB database for CONN."
  (clutch-mongodb-conn-database conn))

(cl-defmethod clutch-db-display-name ((_conn clutch-mongodb-conn))
  "Return \"MongoDB\" as the display name."
  "MongoDB")

(provide 'clutch-mongodb)
;;; clutch-mongodb.el ends here
