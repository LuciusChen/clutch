;;; clutch-db-saphana.el --- SAP HANA JDBC helpers -*- lexical-binding: t; -*-

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

;; SAP HANA-specific helpers layered on top of the JDBC backend.  The
;; JDBC transport, URL construction, and dialect glue live in
;; `clutch-db-jdbc.el'; this file only adds convenience features that
;; do not fit the generic JDBC path — currently the `.hana.gpg'
;; auth-source discovery source, which surfaces each auth-source entry
;; as a virtual saved connection in the query-console picker.
;;
;; Usage:
;;   (setq clutch-saphana-auth-source-files '("~/.hana.gpg"))
;;   ;; M-x clutch-query-console lists every host entry from the file.

;;; Code:

(require 'auth-source)
(require 'cl-lib)
(require 'clutch-connection)
(require 'clutch-db-jdbc)

(defgroup clutch-saphana nil
  "SAP HANA-specific settings for clutch."
  :group 'clutch)

(defcustom clutch-saphana-auth-source-files '("~/.hana.gpg")
  "Auth-source files scanned for virtual SAP HANA saved connections.
Each entry found in these files becomes a virtual saved connection whose
name is the entry's `:host' field.  Discovered connections appear in the
`clutch-query-console' picker after entries in `clutch-connection-alist';
static names in `clutch-connection-alist' always win.

Missing files are silently skipped so the default value does not force
non-HANA users to keep the file on disk.

For safety, the discovery layer only lifts entries that carry both a
resolvable server (via `:server' or `:host') AND a `:port', so a shared
`~/.authinfo.gpg' listing SSH/HTTP tokens will not pollute the HANA
picker with entries that have no port.  Setting a defcustom value like
`clutch-saphana-auth-source-strict-port' to nil is not supported; keep
HANA credentials in a dedicated file."
  :type '(repeat file)
  :group 'clutch-saphana)

;;;; Discovery: parse ~/.hana.gpg into virtual saved connections

(defvar clutch-saphana--auth-entries-cache nil
  "Alist (FILE . (MTIME . ENTRIES)) memoizing decrypted auth-source entries.
Each configured file is decrypted at most once per modification.  The
cache key is a filesystem mtime, so editing `.hana.gpg' invalidates the
entry naturally on the next read.  This keeps the query-console picker
responsive on machines where the GPG agent locks between sessions.")

(defun clutch-saphana--existing-auth-source-files ()
  "Return the subset of `clutch-saphana-auth-source-files' that exists on disk."
  (cl-loop for path in clutch-saphana-auth-source-files
           for expanded = (expand-file-name path)
           when (file-readable-p expanded)
           collect expanded))

(defun clutch-saphana--auth-entries-for-file (file)
  "Return auth-source entries for FILE, memoized by mtime.
FILE is re-read only when its modification time changes."
  (let* ((mtime (file-attribute-modification-time (file-attributes file)))
         (cached (assoc file clutch-saphana--auth-entries-cache)))
    (if (and cached (equal (cadr cached) mtime))
        (cddr cached)
      ;; Bind auth-sources so `auth-source-search' only touches this
      ;; specific file.  Disable auth-source's own cache so the mtime
      ;; check drives freshness.
      (let* ((auth-sources (list file))
             (auth-source-do-cache nil)
             (entries (auth-source-search :max most-positive-fixnum)))
        (setq clutch-saphana--auth-entries-cache
              (cons (cons file (cons mtime entries))
                    (cl-remove file clutch-saphana--auth-entries-cache
                               :key #'car :test #'equal)))
        entries))))

(defun clutch-saphana--auth-entries ()
  "Return raw `auth-source-search' entries from configured HANA files.
Results are memoized per-file by modification time (see
`clutch-saphana--auth-entries-cache')."
  (cl-loop for file in (clutch-saphana--existing-auth-source-files)
           append (clutch-saphana--auth-entries-for-file file)))

(defun clutch-saphana--coerce-port (port-raw)
  "Return a positive integer port from PORT-RAW, or nil when unusable.
Rejects zero, negative numbers, and strings that do not fully parse as
a positive integer — the previous relaxed parser accepted \"12abc\" as
12 and \"-30\" as -30, both of which produce silently wrong connections."
  (cond
   ((null port-raw) nil)
   ((and (integerp port-raw) (> port-raw 0)) port-raw)
   ((stringp port-raw)
    (and (string-match-p "\\`[0-9]+\\'" port-raw)
         (let ((n (string-to-number port-raw)))
           (and (> n 0) n))))))

(defun clutch-saphana--entry-secret (entry)
  "Return the password string from auth-source ENTRY, or nil.
Auth-source stores the secret as a zero-arg thunk (or, less commonly,
a plain string).  This helper resolves it exactly the way `sql-hana.el'
does, so `.hana.gpg' entries authored for either package produce the
same password without a second auth-source lookup."
  (let ((raw (plist-get entry :secret)))
    (cond
     ((null raw) nil)
     ((functionp raw)
      (let ((value (funcall raw)))
        (and (stringp value) (not (string-empty-p value)) value)))
     ((stringp raw)
      (and (not (string-empty-p raw)) raw)))))

(defun clutch-saphana--entry-to-params (entry)
  "Convert auth-source ENTRY into a clutch saphana connection plist.
Returns nil when required fields are missing (no server hostname, no
port).

The `:secret' thunk on ENTRY is resolved eagerly and stored as
`:password' on the returned plist — the same pattern `sql-hana.el' uses.
Eager resolution is safe because the `.hana.gpg' file is already
decrypted at discovery time (auth-source read it to build ENTRY), and
it avoids a second auth-source lookup at connect that would fail
because `~/.hana.gpg' is normally NOT in the global `auth-sources'.

Netrc-style entries (`machine <host> login ...' with no separate
`server' field) are accepted by falling back to the `:host' key when
`:server' is absent — auth-source exposes the `machine' token as
`:host', so treating it as the server matches user expectations for
plain netrc syntax."
  (let* ((server (or (plist-get entry :server)
                     ;; Netrc convention: `machine <host>' — no separate
                     ;; server field.  Fall back to :host so the picker
                     ;; still finds entries that don't spell out `server'.
                     (plist-get entry :host)))
         (port (clutch-saphana--coerce-port (plist-get entry :port)))
         (user (plist-get entry :user))
         (schema (plist-get entry :schema))
         (password (clutch-saphana--entry-secret entry)))
    (when (and (stringp server)
               (not (string-empty-p server))
               ;; Require an explicit port: HANA has no canonical
               ;; default, and unfiltered auth-source files (e.g. a
               ;; shared `.authinfo.gpg' full of SSH tokens) commonly
               ;; have entries without one.  Dropping port-less entries
               ;; keeps the picker HANA-only without a per-entry marker.
               port)
      (append
       (list :backend 'saphana
             :host server
             :port port)
       (when (and (stringp user) (not (string-empty-p user)))
         (list :user user))
       (when (and (stringp schema) (not (string-empty-p schema)))
         (list :schema schema))
       (when password
         (list :password password))))))

(defun clutch-saphana--discovered-connections ()
  "Return an alist of NAME → connection plist for HANA auth-source entries.
NAME comes from the entry's `:host' field (the auth-source nickname).
Duplicates keep the first occurrence.  Entries without a resolvable
server, port, or nickname are dropped."
  (let (result seen)
    (dolist (entry (clutch-saphana--auth-entries))
      (let ((name (plist-get entry :host))
            (params (clutch-saphana--entry-to-params entry)))
        (when (and (stringp name)
                   (not (string-empty-p name))
                   params
                   (not (member name seen)))
          (push name seen)
          (push (cons name params) result))))
    (nreverse result)))

;; Register the discovery source directly.  `clutch-connection' is a
;; hard dependency (see the `require' above), so this backend is not an
;; optional integration and does not need deferred registration.
(add-to-list 'clutch-external-connection-source-functions
             #'clutch-saphana--discovered-connections)

(provide 'clutch-db-saphana)
;;; clutch-db-saphana.el ends here
