;;; clutch-redis.el --- Redis backend adapter -*- lexical-binding: t; -*-

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

;; Basic Redis adapter for Clutch.
;;
;; Redis is exposed as a key/value backend, not as SQL or document storage.  The
;; adapter delegates RESP protocol work to the external redis.el package, then
;; maps Redis commands, key discovery, and type-aware value browsing into
;; Clutch's generic backend contract.

;;; Code:

(require 'cl-lib)
(require 'clutch-backend)
(require 'json)
(require 'seq)
(require 'subr-x)

(declare-function clutch--execute-and-mark "clutch-query" (sql beg end &optional conn))
(declare-function clutch--install-query-keybindings "clutch-query" (map))
(declare-function clutch--query-mode-common-setup "clutch-query" (&optional mode-line-name))
(declare-function redis-command "redis" (conn command &rest arguments))
(declare-function redis-connect "redis" (params))
(declare-function redis-decode-string "redis" (bytes &optional coding))
(declare-function redis-disconnect "redis" (conn))
(declare-function redis-live-p "redis" (conn))
(declare-function redis-conn-database "redis" (conn))
(declare-function redis-conn-host "redis" (conn))
(declare-function redis-conn-port "redis" (conn))

(defvar clutch--query-mode-map)

;;;; Configuration

(defcustom clutch-redis-key-discovery-limit 5000
  "Maximum number of Redis keys collected by one discovery operation.
Redis `SCAN' is incremental, but Clutch's object and completion contracts return
lists.  Stop before those lists can grow without bound."
  :type 'integer
  :group 'clutch)

(defcustom clutch-redis-browse-limit 200
  "Maximum collection elements returned by a generated Redis browse command."
  :type 'integer
  :group 'clutch)

;;;; redis.el API boundary

(defconst clutch-redis--required-redis-functions
  '(redis-command
    redis-connect
    redis-decode-string
    redis-disconnect
    redis-live-p
    redis-conn-database
    redis-conn-host
    redis-conn-port)
  "Public redis.el functions required by the Redis adapter.")

(defun clutch-redis--missing-redis-functions ()
  "Return redis.el functions required by Clutch but currently unavailable."
  (seq-remove #'fboundp clutch-redis--required-redis-functions))

(defun clutch-redis--ensure-redis-client-api ()
  "Load redis.el and verify that its public API is available."
  (unless (featurep 'redis)
    (condition-case err
        (require 'redis)
      (error
       (signal 'clutch-db-error
               (list (format "Redis backend requires redis.el: %s"
                             (error-message-string err)))))))
  (when-let* ((missing (clutch-redis--missing-redis-functions)))
    (signal 'clutch-db-error
            (list (format
                   (concat "Redis backend requires current redis.el public API; "
                           "missing %s. Loaded library: %s. Update/install "
                           "LuciusChen/redis.el, clear stale native-compile cache, "
                           "and restart Emacs.")
                   (mapconcat #'symbol-name missing ", ")
                   (or (locate-library "redis") "not found"))))))

(defmacro clutch-redis--with-redis-errors (&rest body)
  "Run BODY and translate `redis-error' to `clutch-db-error'."
  (declare (indent 0) (debug t))
  `(condition-case err
       (progn ,@body)
     (redis-error
      (signal 'clutch-db-error
              (list (error-message-string err))))))

;;;; Connection

(cl-defstruct clutch-redis-conn
  "A logical Redis connection executed through redis.el."
  params
  client
  closed
  busy)

(defun clutch-redis-connect (params)
  "Connect to Redis using PARAMS."
  (clutch-redis--ensure-redis-client-api)
  (clutch-redis--with-redis-errors
    (make-clutch-redis-conn
     :params (copy-sequence params)
     :client (redis-connect params)
     :closed nil
     :busy nil)))

;;;; Query console mode

(defconst clutch-redis--command-candidates
  '("APPEND" "AUTH" "DBSIZE" "DECR" "DEL" "EXISTS" "EXPIRE" "GET" "HDEL"
    "HEXISTS" "HGET" "HGETALL" "HKEYS" "HLEN" "HMGET" "HRANDFIELD" "HSCAN"
    "HSET" "HVALS" "INCR"
    "KEYS" "LLEN" "LINDEX" "LRANGE" "LPOP" "LPUSH" "MGET" "MSET" "PING"
    "RENAME" "RPUSH" "SADD" "SCAN" "SCARD" "SELECT" "SET" "SISMEMBER"
    "SMEMBERS" "SRANDMEMBER" "SREM" "SSCAN" "TTL" "TYPE" "UNLINK" "XINFO"
    "XLEN" "XRANGE" "ZADD"
    "ZCARD" "ZRANGE" "ZRANGEBYSCORE" "ZREM" "ZREVRANGE" "ZREVRANGEBYSCORE"
    "ZSCAN" "ZSCORE")
  "Redis command completion candidates for `clutch-redis-mode'.")

(defconst clutch-redis-font-lock-keywords
  `((,(regexp-opt clutch-redis--command-candidates 'symbols)
     . font-lock-keyword-face)
    ("\\_<\\(?:WITHSCORES\\|MATCH\\|COUNT\\|NX\\|XX\\|EX\\|PX\\)\\_>"
     . font-lock-constant-face)
    ("#.*$" . font-lock-comment-face))
  "Font-lock rules for `clutch-redis-mode'.")

(defun clutch-redis--command-token-bounds ()
  "Return command-token bounds at point, or nil."
  (when-let* ((bounds (bounds-of-thing-at-point 'symbol))
              (line-prefix (buffer-substring-no-properties
                            (line-beginning-position) (car bounds)))
              ((string-match-p "\\`[[:space:]]*\\'" line-prefix)))
    bounds))

(defun clutch-redis-completion-at-point ()
  "Complete Redis commands at point."
  (when-let* ((bounds (clutch-redis--command-token-bounds)))
    (list (car bounds)
          (cdr bounds)
          clutch-redis--command-candidates
          :exclusive 'no)))

(defun clutch-redis--line-bounds-at-point ()
  "Return non-empty Redis command line bounds around point."
  (save-excursion
    (let ((beg (progn
                 (back-to-indentation)
                 (point)))
          (end (line-end-position)))
      (while (and (> end beg)
                  (memq (char-before end) '(?\s ?\t)))
        (setq end (1- end)))
      (cons beg end))))

(defun clutch-redis-execute-command-at-point ()
  "Execute the Redis command on the current line."
  (interactive)
  (pcase-let* ((`(,beg . ,end) (clutch-redis--line-bounds-at-point))
               (command (string-trim
                         (buffer-substring-no-properties beg end))))
    (when (string-empty-p command)
      (user-error "No Redis command at point"))
    (clutch--execute-and-mark command beg end)))

(defun clutch-redis--install-completion-capfs ()
  "Install Redis completion in the current buffer."
  (remove-hook 'completion-at-point-functions
               #'clutch-redis-completion-at-point t)
  (add-hook 'completion-at-point-functions
            #'clutch-redis-completion-at-point nil t))

(defvar clutch-redis-mode-map
  (make-sparse-keymap)
  "Keymap for `clutch-redis-mode'.")

(defun clutch-redis--ensure-keybindings ()
  "Install Redis query-console key bindings."
  (clutch--install-query-keybindings clutch-redis-mode-map)
  (define-key clutch-redis-mode-map
              (kbd "C-c C-c")
              #'clutch-redis-execute-command-at-point))

;;;###autoload
(define-derived-mode clutch-redis-mode prog-mode "clutch-redis"
  "Major mode for Redis command query buffers.
\\<clutch-redis-mode-map>

\\[clutch-redis-execute-command-at-point]	Execute Redis command at point."
  (clutch-redis--ensure-keybindings)
  (setq-local font-lock-defaults '(clutch-redis-font-lock-keywords))
  (setq-local comment-start "#")
  (setq-local comment-end "")
  (clutch--query-mode-common-setup "clutch-redis")
  (clutch-redis--install-completion-capfs))

;;;; Command parsing and result shaping

(defun clutch-redis--command-parts (command)
  "Return shell-like Redis COMMAND parts."
  (let ((parts (split-string-and-unquote (string-trim command))))
    (unless parts
      (signal 'clutch-db-error (list "Empty Redis command")))
    parts))

(defun clutch-redis--value-cell (value)
  "Return VALUE converted for a Clutch result cell."
  (cond
   ((stringp value) (redis-decode-string value))
   ((numberp value) value)
   ((null value) nil)
   ((listp value)
    (clutch--json-serialize-text
     (mapcar #'clutch-redis--value-cell value)
     "Redis nested response"))
   (t (format "%S" value))))

(defun clutch-redis--string-value (value)
  "Return VALUE as a display string."
  (format "%s" (clutch-redis--value-cell value)))

(defun clutch-redis--columns (&rest names)
  "Return Clutch column metadata for NAMES."
  (mapcar (lambda (name)
            (list :name name :type-category 'text))
          names))

(defun clutch-redis--single-result (conn command value)
  "Return a single-cell result for COMMAND VALUE on CONN."
  (make-clutch-db-result
   :connection conn
   :columns (clutch-redis--columns "result")
   :rows (list (list (clutch-redis--value-cell value)))
   :affected-rows (when (member command '("DEL" "HDEL" "SADD" "SREM" "ZADD" "ZREM"))
                    (and (numberp value) value))))

(defun clutch-redis--indexed-result (conn values)
  "Return an indexed list result for VALUES on CONN."
  (make-clutch-db-result
   :connection conn
   :columns (clutch-redis--columns "index" "value")
   :rows (cl-loop for value in values
                  for index from 0
                  collect (list index (clutch-redis--value-cell value)))))

(defun clutch-redis--pair-result (conn left-name right-name values)
  "Return pair rows using LEFT-NAME and RIGHT-NAME from VALUES on CONN."
  (make-clutch-db-result
   :connection conn
   :columns (clutch-redis--columns left-name right-name)
   :rows (cl-loop for (left right) on values by #'cddr
                  collect (list (clutch-redis--value-cell left)
                                (clutch-redis--value-cell right)))))

(defun clutch-redis--scan-result (conn command value)
  "Return a SCAN-family result for COMMAND VALUE on CONN."
  (pcase-let ((`(,cursor ,items) value))
    (let* ((cursor-cell (clutch-redis--value-cell cursor))
           (pair-scan-p (member command '("HSCAN" "ZSCAN")))
           (columns (if pair-scan-p
                        (clutch-redis--columns "cursor" "name" "value")
                      (clutch-redis--columns "cursor" "value")))
           (rows
            (if pair-scan-p
                (cl-loop for (name item-value) on items by #'cddr
                         collect (list cursor-cell
                                       (clutch-redis--value-cell name)
                                       (clutch-redis--value-cell item-value)))
              (cl-loop for item in items
                       collect (list cursor-cell
                                     (clutch-redis--value-cell item))))))
      ;; Redis permits an empty scan batch before cursor zero.  Preserve that
      ;; continuation cursor instead of rendering a misleading empty result.
      (when (and (null rows) (not (equal cursor-cell "0")))
        (setq rows (list (if pair-scan-p
                             (list cursor-cell nil nil)
                           (list cursor-cell nil)))))
      (make-clutch-db-result
       :connection conn
       :columns columns
       :rows rows))))

(defun clutch-redis--argument-present-p (arguments name)
  "Return non-nil when ARGUMENTS contain Redis option NAME."
  (seq-some (lambda (argument)
              (and (stringp argument)
                   (string-equal (upcase argument) name)))
            arguments))

(defun clutch-redis--zrange-with-scores-p (command arguments)
  "Return non-nil when COMMAND ARGUMENTS request zset scores."
  (and (member command '("ZRANGE" "ZREVRANGE" "ZRANGEBYSCORE" "ZREVRANGEBYSCORE"))
       (clutch-redis--argument-present-p arguments "WITHSCORES")))

(defun clutch-redis--result-from-response (conn command arguments value)
  "Return a `clutch-db-result' for Redis COMMAND ARGUMENTS VALUE on CONN."
  (cond
   ((and (member command '("SCAN" "HSCAN" "SSCAN" "ZSCAN"))
         (consp value))
    (clutch-redis--scan-result conn command value))
   ((and (or (string= command "HGETALL")
             (and (string= command "HRANDFIELD")
                  (clutch-redis--argument-present-p arguments "WITHVALUES")))
         (listp value))
    (clutch-redis--pair-result conn "field" "value" value))
   ((and (clutch-redis--zrange-with-scores-p command arguments)
         (listp value)
         (= (mod (length value) 2) 0))
    (clutch-redis--pair-result conn "member" "score" value))
   ((listp value)
    (clutch-redis--indexed-result conn value))
   (t
    (clutch-redis--single-result conn command value))))

(defun clutch-redis--eval (conn command-text)
  "Execute Redis COMMAND-TEXT on CONN and return command data."
  (pcase-let* ((`(,command . ,arguments)
                 (clutch-redis--command-parts command-text))
               (command (upcase command)))
    (list command
          arguments
          (apply #'redis-command
                 (clutch-redis-conn-client conn)
                 command
                 arguments))))

;;;; Backend methods

(cl-defmethod clutch-db-backend-key ((_conn clutch-redis-conn))
  "Return the registered backend key for Redis connections."
  'redis)

(cl-defmethod clutch-db-init-connection ((_conn clutch-redis-conn))
  "No eager Redis initialization is required.")

(cl-defmethod clutch-db-disconnect ((conn clutch-redis-conn))
  "Close Redis CONN."
  (setf (clutch-redis-conn-closed conn) t)
  (when-let* ((client (clutch-redis-conn-client conn)))
    (redis-disconnect client)))

(cl-defmethod clutch-db-live-p ((conn clutch-redis-conn))
  "Return non-nil when Redis CONN is alive."
  (and (not (clutch-redis-conn-closed conn))
       (redis-live-p (clutch-redis-conn-client conn))))

(cl-defmethod clutch-db-busy-p ((conn clutch-redis-conn))
  "Return non-nil when Redis CONN is executing a command."
  (clutch-redis-conn-busy conn))

(cl-defmethod clutch-db-query ((conn clutch-redis-conn) command-text)
  "Execute Redis COMMAND-TEXT on CONN and return a `clutch-db-result'."
  (setf (clutch-redis-conn-busy conn) t)
  (unwind-protect
      (clutch-redis--with-redis-errors
        (pcase-let ((`(,command ,arguments ,response)
                     (clutch-redis--eval conn command-text)))
          (clutch-redis--result-from-response conn command arguments response)))
    (setf (clutch-redis-conn-busy conn) nil)))

(cl-defmethod clutch-db-result-query-p ((_conn clutch-redis-conn) _command-text)
  "Return non-nil because Redis commands render as result grids."
  t)

(cl-defmethod clutch-db-query-result-context ((_conn clutch-redis-conn) command-text)
  "Return Redis result context for COMMAND-TEXT."
  (list :row-identity-prep (list :sql command-text)
        :server-pageable nil
        :server-rewritable nil))

(cl-defmethod clutch-db-build-paged-sql ((_conn clutch-redis-conn)
                                         command-text _page-num _page-size
                                         &optional _order-by _page-offset)
  "Return COMMAND-TEXT unchanged because Redis commands are not SQL."
  command-text)

(cl-defmethod clutch-db-user ((conn clutch-redis-conn))
  "Return the Redis username for CONN, or nil."
  (plist-get (clutch-redis-conn-params conn) :user))

(cl-defmethod clutch-db-host ((conn clutch-redis-conn))
  "Return the Redis host for CONN."
  (redis-conn-host (clutch-redis-conn-client conn)))

(cl-defmethod clutch-db-port ((conn clutch-redis-conn))
  "Return the Redis port for CONN."
  (redis-conn-port (clutch-redis-conn-client conn)))

(cl-defmethod clutch-db-database ((conn clutch-redis-conn))
  "Return the Redis logical database for CONN."
  (let ((database (redis-conn-database (clutch-redis-conn-client conn))))
    (and database (format "%s" database))))

(cl-defmethod clutch-db-display-name ((_conn clutch-redis-conn))
  "Return the Redis display name."
  "Redis")

(cl-defmethod clutch-db-current-schema ((conn clutch-redis-conn))
  "Return the Redis logical database label for CONN."
  (or (clutch-db-database conn) "0"))

(defun clutch-redis--scan-keys (conn &optional pattern)
  "Return keys from Redis CONN matching PATTERN, up to the discovery limit.
Duplicate keys permitted by Redis `SCAN' are removed while preserving their
first-seen order."
  (unless (and (integerp clutch-redis-key-discovery-limit)
               (> clutch-redis-key-discovery-limit 0))
    (user-error "Redis key discovery limit must be a positive integer"))
  (let ((client (clutch-redis-conn-client conn))
        (cursor "0")
        (seen (make-hash-table :test 'equal))
        keys
        (key-count 0)
        truncated)
    (while (and (not truncated)
                (progn
                  (pcase-let* ((response
                                (if pattern
                                    (redis-command client "SCAN" cursor "MATCH"
                                                   pattern "COUNT" 1000)
                                  (redis-command client "SCAN" cursor "COUNT" 1000)))
                               (`(,next-cursor ,batch) response))
                    (setq cursor (clutch-redis--string-value next-cursor))
                    (dolist (raw-key batch)
                      (let ((key (clutch-redis--string-value raw-key)))
                        (unless (gethash key seen)
                          (if (< key-count clutch-redis-key-discovery-limit)
                              (progn
                                (puthash key t seen)
                                (push key keys)
                                (setq key-count (1+ key-count)))
                            (setq truncated t)))))
                    (when (and (not (string= cursor "0"))
                               (>= key-count clutch-redis-key-discovery-limit))
                      (setq truncated t)))
                  (not (string= cursor "0")))))
    (when truncated
      (message "Redis key discovery stopped at %d keys; narrow the prefix to see more"
               clutch-redis-key-discovery-limit))
    (nreverse keys)))

(cl-defmethod clutch-db-list-tables ((conn clutch-redis-conn))
  "Return Redis key names for CONN."
  (clutch-redis--with-redis-errors
    (clutch-redis--scan-keys conn)))

(defun clutch-redis--key-entry (conn key)
  "Return a Clutch object entry for Redis KEY on CONN."
  (list :name key
        :schema (clutch-db-current-schema conn)
        :type "KEY"))

(cl-defmethod clutch-db-list-table-entries ((conn clutch-redis-conn))
  "Return Redis key entries for CONN."
  (clutch-redis--with-redis-errors
    (mapcar (lambda (key)
              (clutch-redis--key-entry conn key))
            (clutch-redis--scan-keys conn))))

(cl-defmethod clutch-db-search-table-entries ((conn clutch-redis-conn) prefix)
  "Return Redis key entries for CONN matching PREFIX."
  (let ((pattern (if (string-empty-p prefix)
                     "*"
                   (concat prefix "*"))))
    (clutch-redis--with-redis-errors
      (mapcar (lambda (key)
                (clutch-redis--key-entry conn key))
              (clutch-redis--scan-keys conn pattern)))))

(cl-defmethod clutch-db-complete-tables ((conn clutch-redis-conn) prefix)
  "Return Redis key names for CONN matching PREFIX."
  (mapcar (lambda (entry) (plist-get entry :name))
          (clutch-db-search-table-entries conn prefix)))

(cl-defmethod clutch-db-list-columns ((_conn clutch-redis-conn) _key)
  "Return nil because Redis keys do not expose columns."
  nil)

(cl-defmethod clutch-db-object-entry-metadata ((conn clutch-redis-conn) entry)
  "Return Redis ENTRY with value type display metadata from CONN."
  (if (or (plist-get entry :value-type)
          (not (equal (plist-get entry :type) "KEY")))
      entry
    (clutch-redis--with-redis-errors
      (plist-put (copy-sequence entry)
                 :value-type
                 (upcase (clutch-redis--key-type conn
                                                 (plist-get entry :name)))))))

(defun clutch-redis--quoted-argument (value)
  "Return Redis query text for VALUE."
  (prin1-to-string value))

(defun clutch-redis--key-type (conn key)
  "Return Redis type string for KEY on CONN."
  (clutch-redis--string-value
   (redis-command (clutch-redis-conn-client conn) "TYPE" key)))

(cl-defmethod clutch-db-object-browse-query
  ((conn clutch-redis-conn) entry)
  "Return a Redis command to browse key ENTRY on CONN."
  (unless (and (integerp clutch-redis-browse-limit)
               (> clutch-redis-browse-limit 0))
    (user-error "Redis browse limit must be a positive integer"))
  (let* ((key (plist-get entry :name))
         (quoted (clutch-redis--quoted-argument key))
         (type (clutch-redis--with-redis-errors
                 (clutch-redis--key-type conn key))))
    (pcase type
      ("string" (format "GET %s" quoted))
      ("hash" (format "HRANDFIELD %s %d WITHVALUES"
                      quoted clutch-redis-browse-limit))
      ("list" (format "LRANGE %s 0 %d"
                      quoted (1- clutch-redis-browse-limit)))
      ("set" (format "SRANDMEMBER %s %d"
                     quoted clutch-redis-browse-limit))
      ("zset" (format "ZRANGE %s 0 %d WITHSCORES"
                      quoted (1- clutch-redis-browse-limit)))
      ("stream" (format "XRANGE %s - + COUNT %d"
                        quoted clutch-redis-browse-limit))
      (_ (format "TYPE %s" quoted)))))

(cl-defmethod clutch-db-object-details ((conn clutch-redis-conn) entry)
  "Return Redis metadata pairs for key ENTRY on CONN."
  (clutch-redis--with-redis-errors
    (let* ((client (clutch-redis-conn-client conn))
           (key (plist-get entry :name))
           (type (clutch-redis--string-value (redis-command client "TYPE" key)))
           (ttl (redis-command client "TTL" key))
           (exists (redis-command client "EXISTS" key)))
      (delq nil
            `(("Type" . ,type)
              ("TTL" . ,(number-to-string ttl))
              ("Exists" . ,(if (= exists 1) "yes" "no")))))))

(provide 'clutch-redis)
;;; clutch-redis.el ends here
