;;; clutch-test.el --- ERT tests for database workflows -*- lexical-binding: t; -*-

;; Author: Lucius Chen <chenyh572@gmail.com>
;; Maintainer: Lucius Chen <chenyh572@gmail.com>
;; Version: 0.1.0
;; URL: https://github.com/LuciusChen/clutch

;;; Commentary:

;; ERT tests for the clutch user interface layer.
;;
;; Unit tests run without a database server.
;; Native live tests require MySQL and PostgreSQL.  The live runner starts or
;; reuses local containers, preferring Podman on Linux and OrbStack-backed
;; Docker on macOS:
;;   ./test/run-native-live-tests.sh
;;
;; Manual live setup:
;;   docker run -d -e MYSQL_ROOT_PASSWORD=test -p 127.0.0.1:55306:3306 mysql:8
;;   docker run -d -e POSTGRES_INITDB_ARGS=--auth-host=md5 -e POSTGRES_PASSWORD=test -p 127.0.0.1:55432:5432 postgres:16 -c password_encryption=md5
;;
;; Run unit tests from the repository root:
;;   emacs --batch -Q -L . -L test -L ../mysql.el -L ../pg-el \
;;     --eval '(setq load-prefer-newer t)' \
;;     -l ert -l clutch-test \
;;     -f ert-run-tests-batch-and-exit

;;; Code:

(eval-and-compile
  (require 'clutch-test-common))

;;;; Test configuration

(defvar mysql-tls-verify-server)

(defvar clutch-column-displayers)

(defvar clutch--result-source-table)

(defvar clutch--result-server-pageable)

(defvar clutch--result-server-rewritable)

(defvar tramp-rpc-use-controlmaster)

(declare-function make-clutch-jdbc-conn "clutch-db-jdbc" (&rest slot-value-pairs))

(declare-function make-clutch-db-sqlite-conn "clutch-db-sqlite" (&rest slot-value-pairs))

(declare-function make-mysql-conn "mysql" (&rest args))

(declare-function clutch-db-pg--type-category "clutch-db-pg" (oid))

(defvar clutch-test-backend 'mysql)

(defvar clutch-test-host "127.0.0.1")

(defvar clutch-test-port 3306)

(defvar clutch-test-user "root")

(defvar clutch-test-password nil)

(defvar clutch-test-database "mysql")

(defvar clutch-test-url nil
  "Raw JDBC URL for generic live tests.")

(defvar clutch-test-display-name nil
  "Display name for generic JDBC live tests.")

(defvar clutch-test-props nil
  "JDBC connection properties for live tests.")

(require 'clutch-test-sql)
(require 'clutch-test-console)
(require 'clutch-test-object)

;;;; Rendering — value formatting

(ert-deftest clutch-test-format-value-nil ()
  "Test formatting of NULL values."
  (should (equal (clutch--format-value nil) "NULL"))
  (should (equal (clutch--format-value :false) "false")))

(ert-deftest clutch-test-format-value-string ()
  "Test formatting of string values."
  (should (equal (clutch--format-value "hello") "hello"))
  (should (equal (clutch--format-value "") "")))

(ert-deftest clutch-test-format-value-number ()
  "Test formatting of numeric values."
  (should (equal (clutch--format-value 42) "42"))
  (should (equal (clutch--format-value -1) "-1"))
  (should (equal (clutch--format-value 3.14) "3.14")))

(ert-deftest clutch-test-format-value-json-hash-table ()
  "Parsed JSON objects should render as compact JSON strings."
  (let ((ht (make-hash-table :test 'equal)))
    (puthash "key" "val" ht)
    (let ((result (clutch--format-value ht)))
      (should (stringp result))
      (should (string-match-p "\"key\"" result))
      (should (string-match-p "\"val\"" result)))))

(ert-deftest clutch-test-format-value-json-vector ()
  "Parsed JSON arrays should render as compact JSON strings."
  (should (equal (clutch--format-value [1 2 3]) "[1,2,3]")))

(ert-deftest clutch-test-format-value-json-serialization-error-surfaces ()
  "JSON formatting errors should surface as database errors."
  (let ((ht (make-hash-table :test 'equal)))
    (puthash "key" "val" ht)
    (cl-letf (((symbol-function 'json-serialize)
               (lambda (_value)
                 (signal 'wrong-type-argument '("json serialization failed")))))
      (let ((err (should-error (clutch--format-value ht)
                               :type 'clutch-db-error)))
        (should (string-match-p
                 "Cannot serialize query result value as JSON"
                 (cadr err)))))))

(ert-deftest clutch-test-value-to-literal-json-hash-table ()
  "JSON objects should become quoted SQL string literals."
  (let* ((ht (make-hash-table :test 'equal))
         (_ (puthash "k" "v" ht))
         (conn (make-clutch-jdbc-conn
                :params '(:driver sqlserver :user "sa")))
         (result (clutch-db-value-to-literal conn ht #'clutch--format-value)))
    (should (stringp result))
    (should (string-match-p "\"k\"" result))
    (should (string-match-p "\"v\"" result))))

(ert-deftest clutch-test-value-to-literal-json-vector ()
  "JSON arrays should become quoted SQL string literals."
  (let* ((conn (make-clutch-jdbc-conn
                :params '(:driver sqlserver :user "sa")))
         (result (clutch-db-value-to-literal conn [1 2 3] #'clutch--format-value)))
    (should (stringp result))
    (should (string-match-p "1" result))
    (should (string-match-p "2" result))))

(ert-deftest clutch-test-format-value-temporal-plists ()
  "Temporal plists should render through the display formatter."
  (should (equal (clutch--format-value '(:year 2024 :month 3 :day 15))
                 "2024-03-15"))
  (should (equal (clutch--format-value
                  '(:hours 13 :minutes 45 :seconds 30 :negative nil))
                 "13:45:30"))
  (should (equal (clutch--format-value
                  '(:year 2024 :month 3 :day 15
                    :hours 13 :minutes 45 :seconds 30))
                 "2024-03-15 13:45:30")))

(ert-deftest clutch-test-numeric-type-p ()
  "Test numeric column type detection."
  (should (clutch--numeric-type-p '(:name "id" :type-category numeric)))
  (should-not (clutch--numeric-type-p '(:name "name" :type-category text)))
  (should-not (clutch--numeric-type-p '(:name "data" :type-category json))))

(ert-deftest clutch-test-long-field-type-p ()
  "Test long field type detection."
  (should (clutch--long-field-type-p '(:name "content" :type-category blob)))
  (should (clutch--long-field-type-p '(:name "data" :type-category json)))
  (should-not (clutch--long-field-type-p '(:name "id" :type-category numeric)))
  (should-not (clutch--long-field-type-p '(:name "name" :type-category text))))

(ert-deftest clutch-test-json-value-to-string-hash-table ()
  "JSON viewer should accept parsed JSON objects."
  (let ((obj (make-hash-table :test 'equal)))
    (puthash "a" 1 obj)
    (should (equal (clutch--json-value-to-string obj) "{\"a\":1}"))))

(ert-deftest clutch-test-json-value-to-string-hash-table-preserves-unicode ()
  "Parsed JSON objects should keep readable Unicode text."
  (let ((obj (make-hash-table :test 'equal)))
    (puthash "quote" "记忆碎片已封存" obj)
    (puthash "operator" "Saito" obj)
    (puthash "thermoptic" :false obj)
    (should (equal (clutch--json-value-to-string obj)
                   "{\"quote\":\"记忆碎片已封存\",\"operator\":\"Saito\",\"thermoptic\":false}"))))

(ert-deftest clutch-test-json-value-to-string-scalars ()
  "JSON scalar values should remain valid JSON when edited or viewed."
  (should (equal (clutch--json-value-to-string "hello") "\"hello\""))
  (should (equal (clutch--json-value-to-string nil) "null"))
  (should (equal (clutch--json-value-to-string t) "true"))
  (should (equal (clutch--json-value-to-string :false) "false"))
  (should (equal (clutch--json-value-to-string 42) "42")))

(ert-deftest clutch-test-dispatch-view-json-category-serializes-non-string ()
  "JSON category should route non-string values to JSON viewer."
  (let (seen buffer-name)
    (cl-letf (((symbol-function 'clutch--view-in-buffer)
               (lambda (content name _setup)
                 (setq seen content
                       buffer-name name)))
              ((symbol-function 'clutch--json-value-to-string)
               (lambda (_val) "{\"ok\":true}")))
      (clutch--dispatch-view (vector 1 2) '(:type-category json))
      (should (equal seen "{\"ok\":true}"))
      (should (equal buffer-name "*clutch-json*")))))

(ert-deftest clutch-test-dispatch-view-json-string-bypasses-serialize ()
  "String vals should be passed directly to the JSON viewer without re-serialization.
This avoids `json-serialize' escaping non-ASCII characters (e.g. CJK) as \\uXXXX."
  (let ((seen nil)
        (serialize-called nil))
    (cl-letf (((symbol-function 'clutch--view-in-buffer)
               (lambda (content _name _setup) (setq seen content)))
              ((symbol-function 'clutch--json-value-to-string)
               (lambda (_v) (setq serialize-called t) "{}")))
      (clutch--dispatch-view "{\"name\":\"张三\"}" '(:type-category json))
      (should (equal seen "{\"name\":\"张三\"}"))
      (should-not serialize-called))))

(ert-deftest clutch-test-json-view-mode-falls-back-when-json-ts-errors ()
  "JSON viewers should tolerate missing tree-sitter grammars."
  (let (selected-mode)
    (with-temp-buffer
      (insert "{\"ok\":true}")
      (cl-letf (((symbol-function 'json-ts-mode)
                 (lambda () (error "missing JSON grammar")))
                ((symbol-function 'json-mode)
                 (lambda () (setq selected-mode 'json-mode)))
                ((symbol-function 'js-mode)
                 (lambda () (setq selected-mode 'js-mode))))
        (clutch--setup-json-view-buffer)))
    (should (eq selected-mode 'json-mode))))

(ert-deftest clutch-test-dispatch-view-fallback-to-plain ()
  "Unknown values should open plain viewer rather than JSON viewer."
  (let (buffer-name content)
    (cl-letf (((symbol-function 'clutch--view-in-buffer)
               (lambda (text name _setup)
                 (setq content text
                       buffer-name name))))
      (clutch--dispatch-view "hello" '(:type-category text))
      (should (equal content "hello"))
      (should (equal buffer-name "*clutch-value*")))))

(ert-deftest clutch-test-dispatch-view-xml-content-overrides-blob ()
  "XML-like content should use XML viewer even when column type is blob."
  (let (buffer-name)
    (cl-letf (((symbol-function 'clutch--view-in-buffer)
               (lambda (_text name _setup) (setq buffer-name name))))
      (clutch--dispatch-view "<rss><item>1</item></rss>" '(:type-category blob))
      (should (equal buffer-name "*clutch-xml*")))))

(ert-deftest clutch-test-dispatch-view-invalid-angle-text-not-xml ()
  "Invalid XML-like text should not be forced into XML viewer."
  (let (buffer-name)
    (cl-letf (((symbol-function 'clutch--view-in-buffer)
               (lambda (_text name _setup) (setq buffer-name name))))
      (clutch--dispatch-view "<abc" '(:type-category text))
      (should (equal buffer-name "*clutch-value*")))))

(ert-deftest clutch-test-blob-view-string-has-size-and-hex ()
  "Blob preview should include size and hex output."
  (let ((s (clutch--blob-view-string (unibyte-string #x00 #xff #x41 #x7f))))
    (should (string-match-p "BLOB size: 4 bytes" s))
    (should (string-match-p "Hex preview:" s))
    (should (string-match-p "00 ff 41 7f" s))))

(ert-deftest clutch-test-blob-view-string-text-preview ()
  "Text-like blobs should use concise text preview."
  (let ((s (clutch--blob-view-string "hello world")))
    (should (string-match-p "BLOB size: 11 bytes" s))
    (should (string-match-p "Text preview:" s))
    (should-not (string-match-p "Hex preview:" s))))

(ert-deftest clutch-test-value-placeholder-detects-xml-and-blob ()
  "Grid placeholders should compactly mark XML and BLOB values."
  (should (equal (clutch--value-placeholder "<root/>" '(:type-category text))
                 "<XML>"))
  (should (equal (clutch--value-placeholder (unibyte-string #x00 #x01)
                                            '(:type-category blob))
                 "<BLOB>")))

(ert-deftest clutch-test-xml-like-string-p-strict ()
  "XML detection should avoid false positives for plain angle-bracket text."
  (should (clutch--xml-like-string-p "<rss><item>1</item></rss>"))
  (should (clutch--xml-like-string-p "<?xml version=\"1.0\"?><rss/>"))
  (should-not (clutch--xml-like-string-p "<abc"))
  (should-not (clutch--xml-like-string-p "just <text> marker")))

(ert-deftest clutch-test-decode-xml-char-refs-string ()
  "XML viewer helper should decode numeric character references."
  (should (equal
           (clutch--decode-xml-char-refs-string
            "<zone>&#x6E7E;&#x5CB8;</zone><operator>&#25998;&#34276;</operator>")
           "<zone>湾岸</zone><operator>斎藤</operator>")))

(ert-deftest clutch-test-view-xml-value-enables-fontification ()
  "XML viewer should invoke fontification and show byte size in header."
  (let ((fontified nil)
        (buf nil))
    (cl-letf (((symbol-function 'executable-find) (lambda (_cmd) nil))
              ((symbol-function 'nxml-mode) (lambda () nil))
              ((symbol-function 'font-lock-ensure)
               (lambda (&rest _args) (setq fontified t)))
              ((symbol-function 'jit-lock-fontify-now)
               (lambda (&rest _args) nil))
              ((symbol-function 'pop-to-buffer)
               (lambda (b &rest _args)
                 (setq buf b)
                 b)))
      (clutch--dispatch-view "<root><a>1</a></root>" '(:type-category text))
      (should fontified)
      (with-current-buffer buf
        (should (string-match-p "XML" (format "%s" header-line-format)))
        (should (string-match-p "bytes" (format "%s" header-line-format)))))))

(ert-deftest clutch-test-view-xml-value-decodes-numeric-char-refs-for-display ()
  "XML viewer should display numeric character references as UTF-8 text."
  (let ((buf nil))
    (cl-letf (((symbol-function 'executable-find) (lambda (_cmd) nil))
              ((symbol-function 'nxml-mode) (lambda () nil))
              ((symbol-function 'font-lock-ensure) (lambda (&rest _args) nil))
              ((symbol-function 'jit-lock-fontify-now) (lambda (&rest _args) nil))
              ((symbol-function 'pop-to-buffer)
               (lambda (b &rest _args)
                 (setq buf b)
                 b)))
      (clutch--dispatch-view
       "<?xml version=\"1.0\"?><overlay><zone>&#x6E7E;&#x5CB8;&#x30B1;&#x30FC;&#x30D6;&#x30EB;&#x7DB2;</zone><operator>&#x658E;&#x85E4;</operator></overlay>"
       '(:type-category text))
      (with-current-buffer buf
        (let ((text (buffer-string)))
          (should (string-match-p "湾岸ケーブル網" text))
          (should (string-match-p "斎藤" text))
          (should-not (string-match-p "&#x6E7E;" text)))))))

(ert-deftest clutch-test-value-to-literal-nil ()
  "Test NULL literal conversion."
  (should (equal (clutch-db-value-to-literal 'fake-conn nil) "NULL")))

(ert-deftest clutch-test-value-to-literal-number ()
  "Test numeric literal conversion."
  (should (equal (clutch-db-value-to-literal 'fake-conn 42) "42"))
  (should (string-match-p "3\\.14"
                          (clutch-db-value-to-literal 'fake-conn 3.14)))
  (should (equal (clutch-db-value-to-literal 'fake-conn -1) "-1")))

(ert-deftest clutch-test-value-to-literal-string ()
  "Test string literal conversion (requires connection)."
  (require 'clutch-db-mysql)
  (require 'mysql)
  ;; String escaping requires a connection
  (let ((conn (make-mysql-conn :host "localhost")))
    (let ((result (clutch-db-value-to-literal conn "hello")))
      (should (stringp result))
      (should (string-prefix-p "'" result)))
    (let ((result (clutch-db-value-to-literal conn "it's")))
      (should (string-match-p "\\\\'" result)))))

;;;; Rendering — padding

(ert-deftest clutch-test-string-pad ()
  "Test string padding."
  ;; Left-align (default)
  (should (equal (clutch--string-pad "hi" 5) "hi   "))
  ;; Right-align
  (should (equal (clutch--string-pad "hi" 5 t) "   hi"))
  ;; String longer than width — no padding
  (should (equal (clutch--string-pad "hello" 3) "hello")))

;;;; Rendering — column layout and widths

(ert-deftest clutch-test-column-names ()
  "Test column name extraction from column definitions."
  (let ((columns (list '(:name "id" :type-category numeric)
                       '(:name "name" :type-category text)
                       '(:name "data" :type-category json))))
    (let ((names (clutch-db-result-column-names columns)))
      (should (equal names '("id" "name" "data"))))))

(ert-deftest clutch-test-compute-column-widths ()
  "Test column width computation."
  (let* ((col-names '("id" "name" "email"))
         (rows '((1 "alice" "alice@example.com")
                 (2 "bob" "bob@example.com")))
         (columns '((:name "id" :type-category numeric)
                    (:name "name" :type-category text)
                    (:name "email" :type-category text)))
         (widths (clutch--compute-column-widths col-names rows columns)))
    (should (vectorp widths))
    (should (= (length widths) 3))
    ;; id: max(2, 1) = 2
    (should (>= (aref widths 0) 2))
    ;; name: max(4, 5) = 5 (alice)
    (should (>= (aref widths 1) 5))
    ;; email: max(5, 17) = 17 (alice@example.com)
    (should (>= (aref widths 2) 5))))

(ert-deftest clutch-test-compute-column-widths-with-max ()
  "Test column width computation respects max width."
  (let* ((clutch-column-width-max 10)
         (col-names '("description"))
         (rows '(("this is a very long description that exceeds the maximum width")))
         (columns '((:name "description" :type-category text)))
         (widths (clutch--compute-column-widths col-names rows columns)))
    ;; Should be capped at max width
    (should (<= (aref widths 0) clutch-column-width-max))))

(ert-deftest clutch-test-visible-columns-renders-all-result-columns ()
  "The result buffer should render every result column into one table."
  (with-temp-buffer
    (setq-local clutch--result-columns '("c1" "c2" "c3" "c4"))
    (should (equal (clutch--visible-columns) '(0 1 2 3)))))

(ert-deftest clutch-test-visible-columns-skips-hidden-row-identity-columns ()
  "Hidden row identity columns should not be rendered as user data."
  (with-temp-buffer
    (setq-local clutch--result-columns '("clutch__rid_0" "id" "name")
                clutch--result-column-defs
                '((:name "clutch__rid_0" :hidden t)
                  (:name "id")
                  (:name "name")))
    (should (equal (clutch--visible-columns) '(1 2)))))

(ert-deftest clutch-test-visible-column-names-skip-hidden-columns ()
  "User-facing column choices should not include hidden row identity aliases."
  (with-temp-buffer
    (setq-local clutch--result-columns '("clutch__rid_0" "id" "name")
                clutch--result-column-defs
                '((:name "clutch__rid_0" :hidden t)
                  (:name "id")
                  (:name "name")))
    (insert (propertize "name" 'clutch-col-idx 2))
    (goto-char (point-min))
    (let (candidates default)
      (cl-letf (((symbol-function 'completing-read)
                 (lambda (_prompt collection &optional _predicate _require-match
                                   _initial-input _hist def _inherit-input-method)
                   (setq candidates collection
                         default def)
                   def)))
        (should (equal (clutch-result--read-column) "name"))
        (should (equal candidates '("id" "name")))
        (should (equal default "name"))))))

(ert-deftest clutch-test-row-identity-prep-injects-hidden-primary-key ()
  "Simple table SELECTs should receive hidden identity expressions."
  (cl-letf (((symbol-function 'clutch-db-row-identity-candidates)
             (lambda (_conn _table)
               (list (list :kind 'primary-key
                           :name "PRIMARY"
                           :columns '("id")))))
            ((symbol-function 'clutch-db-escape-identifier)
             (lambda (_conn id) (format "\"%s\"" id))))
    (let ((prep (clutch--prepare-row-identity-query
                 'fake-conn "SELECT name FROM users WHERE active = 1")))
      (should (plist-get prep :augmented))
      (should (equal (plist-get prep :hidden-aliases) '("clutch__rid_0")))
      (should (equal (plist-get prep :sql)
                     "SELECT name, \"id\" AS \"clutch__rid_0\" FROM users WHERE active = 1")))))

(ert-deftest clutch-test-row-identity-prep-uses-backend-source-table-name ()
  "Row identity preparation should canonicalize source tables through the backend."
  (let (requested-table)
    (cl-letf (((symbol-function 'clutch-db--source-table-name)
               (lambda (_conn token)
                 (should (equal token "users"))
                 "USERS"))
              ((symbol-function 'clutch-db-row-identity-candidates)
               (lambda (_conn table)
                 (setq requested-table table)
                 (list (list :kind 'primary-key
                             :name "PRIMARY"
                             :columns '("ID")))))
              ((symbol-function 'clutch-db-escape-identifier)
               (lambda (_conn id) (format "\"%s\"" id))))
      (let ((prep (clutch--prepare-row-identity-query
                   'fake-conn "SELECT name FROM users")))
        (should (equal requested-table "USERS"))
        (should (equal (plist-get prep :table) "USERS"))))))

(ert-deftest clutch-test-row-identity-prep-select-star-qualifies-star ()
  "SELECT * row identity injection should qualify the star before adding columns."
  (cl-letf (((symbol-function 'clutch-db-row-identity-candidates)
             (lambda (_conn _table)
               (list (list :kind 'row-locator
                           :name "ROWID"
                           :select-expressions '("ROWID")
                           :where-sql "ROWID = ?"))))
            ((symbol-function 'clutch-db-escape-identifier)
             (lambda (_conn id) (format "\"%s\"" id))))
    (dolist (case '(("SELECT * FROM users WHERE active = 1"
                     "SELECT users.*, ROWID AS \"clutch__rid_0\" FROM users WHERE active = 1")
                    ("SELECT * FROM users WHERE users.active = 1"
                     "SELECT users.*, ROWID AS \"clutch__rid_0\" FROM users WHERE users.active = 1")
                    ("SELECT * FROM users;"
                     "SELECT users.*, ROWID AS \"clutch__rid_0\" FROM users")
                    ("SELECT * FROM users u WHERE active = 1"
                     "SELECT u.*, ROWID AS \"clutch__rid_0\" FROM users u WHERE active = 1")))
      (let ((prep (clutch--prepare-row-identity-query
                   'oracle-conn (car case))))
        (should (plist-get prep :augmented))
        (should (equal (plist-get prep :sql) (cadr case)))))))

(ert-deftest clutch-test-row-identity-prep-normalizes-leading-comments ()
  "Leading comments should not disable row identity injection."
  (cl-letf (((symbol-function 'clutch-db-row-identity-candidates)
             (lambda (_conn _table)
               (list (list :kind 'primary-key
                           :name "PRIMARY"
                           :columns '("id")))))
            ((symbol-function 'clutch-db-escape-identifier)
             (lambda (_conn id) (format "\"%s\"" id))))
    (let ((prep (clutch--prepare-row-identity-query
                 'fake-conn "-- comment\nSELECT * FROM users;")))
      (should (plist-get prep :augmented))
      (should (equal (plist-get prep :sql)
                     "SELECT users.*, \"id\" AS \"clutch__rid_0\" FROM users")))))

(ert-deftest clutch-test-row-identity-prep-skips-aggregate-selects ()
  "Aggregate SELECTs should not receive hidden row identity expressions."
  (cl-letf (((symbol-function 'clutch-db-row-identity-candidates)
             (lambda (_conn _table)
               (list (list :kind 'primary-key
                           :name "PRIMARY"
                           :columns '("id")))))
            ((symbol-function 'clutch-db-escape-identifier)
             (lambda (_conn id) (format "\"%s\"" id))))
    (dolist (sql '("SELECT count(1) FROM users"
                   "SELECT COUNT(*) AS n FROM users"
                   "SELECT count(*) FILTER (WHERE active) FROM users"
                   "SELECT max(id) FROM users WHERE active = 1"
                   "SELECT coalesce(sum(amount), 0) AS total FROM orders"
                   "SELECT listagg(name, ',') WITHIN GROUP (ORDER BY name) FROM users"))
      (let ((prep (clutch--prepare-row-identity-query 'fake-conn sql)))
        (should-not (plist-get prep :augmented))
        (should (equal (plist-get prep :sql) sql))))))

(ert-deftest clutch-test-row-identity-prep-skips-aggregate-selects-for-row-locators ()
  "Aggregate SELECTs should not receive ROWID/ctid-style locator expressions."
  (cl-letf (((symbol-function 'clutch-db-row-identity-candidates)
             (lambda (_conn _table)
               (list (list :kind 'row-locator
                           :name "ROWID"
                           :select-expressions '("ROWID")
                           :where-sql "ROWID = ?"))))
            ((symbol-function 'clutch-db-escape-identifier)
             (lambda (_conn id) (format "\"%s\"" id))))
    (let* ((sql "SELECT COUNT(*) FROM users")
           (prep (clutch--prepare-row-identity-query 'oracle-conn sql)))
      (should-not (plist-get prep :augmented))
      (should (equal (plist-get prep :sql) sql)))))

(ert-deftest clutch-test-row-identity-prep-allows-window-aggregate-selects ()
  "Window aggregates are row-preserving and may receive hidden identity columns."
  (cl-letf (((symbol-function 'clutch-db-row-identity-candidates)
             (lambda (_conn _table)
               (list (list :kind 'primary-key
                           :name "PRIMARY"
                           :columns '("id")))))
            ((symbol-function 'clutch-db-escape-identifier)
             (lambda (_conn id) (format "\"%s\"" id))))
    (dolist (case '(("SELECT name, count(*) OVER () AS total FROM users"
                     "SELECT name, count(*) OVER () AS total, \"id\" AS \"clutch__rid_0\" FROM users")
                    ("SELECT name, sum(score) FILTER (WHERE score > 0) OVER () AS total FROM users"
                     "SELECT name, sum(score) FILTER (WHERE score > 0) OVER () AS total, \"id\" AS \"clutch__rid_0\" FROM users")))
      (let ((prep (clutch--prepare-row-identity-query
                   'fake-conn (car case))))
        (should (plist-get prep :augmented))
        (should (equal (plist-get prep :sql) (cadr case)))))))

(ert-deftest clutch-test-row-identity-prep-ignores-aggregate-in-scalar-subquery ()
  "Aggregates inside scalar subqueries should not block outer row identity."
  (cl-letf (((symbol-function 'clutch-db-row-identity-candidates)
             (lambda (_conn _table)
               (list (list :kind 'primary-key
                           :name "PRIMARY"
                           :columns '("id")))))
            ((symbol-function 'clutch-db-escape-identifier)
             (lambda (_conn id) (format "\"%s\"" id))))
    (let ((prep (clutch--prepare-row-identity-query
                 'fake-conn
                 "SELECT name, (SELECT count(*) FROM orders) AS order_count FROM users")))
      (should (plist-get prep :augmented))
      (should (equal (plist-get prep :sql)
                     "SELECT name, (SELECT count(*) FROM orders) AS order_count, \"id\" AS \"clutch__rid_0\" FROM users")))))

(ert-deftest clutch-test-row-identity-prep-preserves-ordinal-order-by ()
  "Hidden identity expressions should not change ORDER BY ordinals."
  (cl-letf (((symbol-function 'clutch-db-row-identity-candidates)
             (lambda (_conn _table)
               (list (list :kind 'primary-key
                           :name "PRIMARY"
                           :columns '("id")))))
            ((symbol-function 'clutch-db-escape-identifier)
             (lambda (_conn id) (format "\"%s\"" id))))
    (let ((prep (clutch--prepare-row-identity-query
                 'fake-conn "SELECT name, status FROM users ORDER BY 1")))
      (should (plist-get prep :augmented))
      (should (equal (plist-get prep :sql)
                     "SELECT name, status, \"id\" AS \"clutch__rid_0\" FROM users ORDER BY 1")))))

(ert-deftest clutch-test-row-identity-prep-skips-joined-selects ()
  "Joined SELECTs should not receive table-local hidden identity columns."
  (cl-letf (((symbol-function 'clutch-db-row-identity-candidates)
             (lambda (_conn _table)
               (list (list :kind 'primary-key
                           :name "PRIMARY"
                           :columns '("id")))))
            ((symbol-function 'clutch-db-escape-identifier)
             (lambda (_conn id) (format "\"%s\"" id))))
    (let* ((sql "SELECT u.name, o.total FROM users u JOIN orders o ON o.user_id = u.id")
           (prep (clutch--prepare-row-identity-query 'fake-conn sql)))
      (should-not (plist-get prep :augmented))
      (should (equal (plist-get prep :sql) sql)))))

(ert-deftest clutch-test-row-identity-prep-skips-comma-joins-and-derived-selects ()
  "Ambiguous or derived SELECTs should not receive hidden identity columns."
  (cl-letf (((symbol-function 'clutch-db-row-identity-candidates)
             (lambda (_conn _table)
               (list (list :kind 'primary-key
                           :name "PRIMARY"
                           :columns '("id")))))
            ((symbol-function 'clutch-db-escape-identifier)
             (lambda (_conn id) (format "\"%s\"" id))))
    (dolist (sql '("SELECT * FROM users, orders"
                   "WITH x AS (SELECT * FROM users) SELECT * FROM x"
                   "SELECT * FROM (SELECT * FROM users) u"))
      (let ((prep (clutch--prepare-row-identity-query 'fake-conn sql)))
        (should-not (plist-get prep :augmented))
        (should (equal (plist-get prep :sql) sql))))))

(ert-deftest clutch-test-row-identity-finalize-separates-hidden-and-source-pk ()
  "Hidden locator indices and visible source PK indices should stay distinct."
  (let* ((prep (list :table "users"
                     :candidate (list :kind 'primary-key
                                      :name "PRIMARY"
                                      :columns '("id"))
                     :hidden-aliases '("clutch__rid_0")
                     :augmented t))
         (columns (clutch--apply-row-identity-column-metadata
                   '((:name "id") (:name "name") (:name "clutch__rid_0"))
                   prep))
         (row-identity (clutch--finalize-row-identity prep columns)))
    (should (plist-get (nth 2 columns) :hidden))
    (should (equal (plist-get row-identity :indices) '(2)))
    (should (equal (plist-get row-identity :source-indices) '(0)))))

(ert-deftest clutch-test-render-result-includes-all-columns ()
  "Wide tables should keep later columns searchable and reachable by TAB."
  (with-temp-buffer
    (clutch-result-mode)
    (setq-local clutch--result-columns '("c1" "c2" "c3" "c4"))
    (setq-local clutch--result-column-defs '(nil nil nil nil))
    (setq-local clutch--result-rows '(("a" "b" "c" "needle")))
    (setq-local clutch--filtered-rows nil)
    (setq-local clutch--pending-edits nil)
    (setq-local clutch--pending-deletes nil)
    (setq-local clutch--pending-inserts nil)
    (setq-local clutch--marked-rows nil)
    (setq-local clutch--sort-column nil)
    (setq-local clutch--sort-descending nil)
    (setq-local clutch--page-current 0)
    (setq-local clutch--page-total-rows 1)
    (setq-local clutch--column-widths [5 5 5 6])
    (clutch--refresh-display)
    (should (string-match-p "needle" (buffer-string)))
    (goto-char (point-min))
    (let ((first (text-property-search-forward 'clutch-col-idx 0 #'eq)))
      (should first)
      (goto-char (prop-match-beginning first)))
    (clutch-result-next-cell)
    (should (= (get-text-property (point) 'clutch-col-idx) 1))
    (clutch-result-next-cell)
    (should (= (get-text-property (point) 'clutch-col-idx) 2))
    (clutch-result-next-cell)
    (should (= (get-text-property (point) 'clutch-col-idx) 3))))

(ert-deftest clutch-test-goto-cell-uses-row-start-positions ()
  "Cell navigation should use cached row starts when available."
  (with-temp-buffer
    (insert "row0\nrow1\n")
    (let* ((row0 (point-min))
           (row1 (save-excursion
                   (goto-char (point-min))
                   (forward-line 1)
                   (point)))
           (clutch--row-start-positions (vector row0 row1)))
      (add-text-properties (+ row0 1) (+ row0 2)
                           '(clutch-row-idx 0 clutch-col-idx 0))
      (add-text-properties (+ row1 2) (+ row1 3)
                           '(clutch-row-idx 1 clutch-col-idx 7))
      (clutch--goto-cell 1 7)
      (should (= (point) (+ row1 2))))))

(ert-deftest clutch-test-goto-cell-falls-back-to-first-cell-in-row ()
  "Cell navigation should fall back to the first cell on the target row."
  (with-temp-buffer
    (insert "row0\nrow1\n")
    (let* ((row0 (point-min))
           (row1 (save-excursion
                   (goto-char (point-min))
                   (forward-line 1)
                   (point)))
           (clutch--row-start-positions (vector row0 row1)))
      (add-text-properties (+ row1 3) (+ row1 4)
                           '(clutch-row-idx 1 clutch-col-idx 2))
      (clutch--goto-cell 1 99)
      (should (= (point) (+ row1 3))))))

;;;; Rendering — row and separator rendering

(ert-deftest clutch-test-refresh-display-preserves-visible-row-position ()
  "Refreshing the result view should not drift point downward on screen."
  (save-window-excursion
    (let ((buf (get-buffer-create " *clutch-refresh-display*")))
      (unwind-protect
          (progn
            (switch-to-buffer buf)
            (with-current-buffer buf
              (clutch-result-mode)
              (setq-local clutch--result-columns '("c1" "c2" "c3"))
              (setq-local clutch--result-column-defs '(nil nil nil))
              (setq-local clutch--result-rows
                          (cl-loop for i from 1 to 40
                                   collect (list (format "row%02d-a" i)
                                                 (format "row%02d-b" i)
                                                 (format "row%02d-c" i))))
              (setq-local clutch--filtered-rows nil)
              (setq-local clutch--pending-edits nil)
              (setq-local clutch--pending-deletes nil)
              (setq-local clutch--pending-inserts nil)
              (setq-local clutch--marked-rows nil)
              (setq-local clutch--sort-column nil)
              (setq-local clutch--sort-descending nil)
              (setq-local clutch--page-current 0)
              (setq-local clutch--page-total-rows 40)
              (setq-local clutch--column-widths [8 8 8])
              (clutch--refresh-display)
              (let* ((win (selected-window))
                     (top-ridx 10)
                     (point-ridx 15))
                (set-window-start win (aref clutch--row-start-positions top-ridx))
                (goto-char (aref clutch--row-start-positions point-ridx))
                (forward-char 2)
                (let ((before-top-ridx
                       (save-excursion
                         (goto-char (window-start win))
                         (clutch--row-idx-at-line)))
                      (before-line
                       (count-screen-lines (window-start win)
                                           (line-beginning-position))))
                  (clutch--refresh-display)
                  (should (= (save-excursion
                               (goto-char (window-start win))
                               (clutch--row-idx-at-line))
                             before-top-ridx))
                  (should (= (count-screen-lines (window-start win)
                                                 (line-beginning-position))
                             before-line))
                  (clutch--refresh-display)
                  (should (= (save-excursion
                               (goto-char (window-start win))
                               (clutch--row-idx-at-line))
                             before-top-ridx))
                  (should (= (count-screen-lines (window-start win)
                                                 (line-beginning-position))
                             before-line))))))
        (when (buffer-live-p buf)
          (kill-buffer buf))))))

(defun clutch-test--setup-rendered-result (&optional rows)
  "Populate the current buffer with a rendered three-column result table.
ROWS defaults to a small three-row sample."
  (let ((rows (or rows '((1 "alpha" "oslo")
                         (2 "bravo" "rome")
                         (3 "charlie" "paris"))))
        (columns '((:name "id" :type-category numeric)
                   (:name "name" :type-category text)
                   (:name "city" :type-category text))))
    (clutch-result-mode)
    (setq-local clutch--result-columns '("id" "name" "city")
                clutch--result-column-defs columns
                clutch--result-rows rows
                clutch--filtered-rows nil
                clutch--pending-edits nil
                clutch--pending-deletes nil
                clutch--pending-inserts nil
                clutch--marked-rows nil
                clutch--row-identity (clutch-test--primary-row-identity
                                       "users" '("id") '(0))
                clutch--sort-column nil
                clutch--sort-descending nil
                clutch--page-current 0
                clutch--page-total-rows (length rows)
                clutch--query-elapsed nil
                clutch-result-max-rows 100
                clutch--column-widths [3 8 8])
    (clutch--render-result)))

(defun clutch-test--rendered-line-at (ridx)
  "Return rendered line RIDX from the current result buffer."
  (let ((start (aref clutch--row-start-positions ridx))
        (end (or (and (< (1+ ridx) (length clutch--row-start-positions))
                      (aref clutch--row-start-positions (1+ ridx)))
                 (point-max))))
    (buffer-substring start end)))

(ert-deftest clutch-test-replace-row-at-index-changes-only-target-row ()
  "Row replacement should update only the targeted rendered line."
  (with-temp-buffer
    (clutch-test--setup-rendered-result)
    (let ((before0 (substring-no-properties (clutch-test--rendered-line-at 0)))
          (before1 (substring-no-properties (clutch-test--rendered-line-at 1)))
          (before2 (substring-no-properties (clutch-test--rendered-line-at 2))))
      (setq-local clutch--pending-deletes (list (vector 2)))
      (clutch--replace-row-at-index 1)
      (let ((after0 (substring-no-properties (clutch-test--rendered-line-at 0)))
            (after1 (substring-no-properties (clutch-test--rendered-line-at 1)))
            (after2 (substring-no-properties (clutch-test--rendered-line-at 2))))
        (should (equal before0 after0))
        (should (equal before2 after2))
        (should-not (equal before1 after1))
        (should (string-match-p "^│D" after1))))))

(ert-deftest clutch-test-replace-row-at-index-preserves-point-cell ()
  "Row replacement should keep point on the same logical cell."
  (with-temp-buffer
    (clutch-test--setup-rendered-result)
    (setq-local clutch--pending-edits
                (list (cons (cons (vector 2) 2) "表表表")))
    (clutch--goto-cell 1 2)
    (clutch--replace-row-at-index 1)
    (should (= (get-text-property (point) 'clutch-row-idx) 1))
    (should (= (get-text-property (point) 'clutch-col-idx) 2))))

(ert-deftest clutch-test-replace-row-at-index-falls-back-without-row-starts ()
  "Row replacement should fall back to a full redraw without cached row starts."
  (with-temp-buffer
    (let (refreshed)
      (setq-local clutch--result-rows '((1 "alpha" "oslo")))
      (setq-local clutch--filtered-rows nil)
      (setq-local clutch--column-widths [3 8 8])
      (cl-letf (((symbol-function 'clutch--refresh-display)
                 (lambda ()
                   (setq refreshed t))))
        (clutch--replace-row-at-index 0)
        (should refreshed)))))

(ert-deftest clutch-test-reindex-row-starts-from-tracks-buffer-positions ()
  "Row-start reindexing should match actual buffer positions after replacement."
  (with-temp-buffer
    (clutch-test--setup-rendered-result)
    (let ((old-third-start (aref clutch--row-start-positions 2)))
      (setq-local clutch--pending-edits
                  (list (cons (cons (vector 2) 2) "表表表")))
      (clutch--replace-row-at-index 1)
      (let ((actual-third-start (save-excursion
                                  (goto-char (point-min))
                                  (forward-line 2)
                                  (point))))
        (should (= (aref clutch--row-start-positions 2) actual-third-start))
        (should (/= old-third-start actual-third-start))))))

(ert-deftest clutch-test-render-row-line-matches-inserted-output ()
  "Row-line rendering should match the line inserted into the result buffer."
  (with-temp-buffer
    (clutch-test--setup-rendered-result)
    (let* ((render-state (clutch--build-render-state))
           (expected (clutch-test--rendered-line-at 1))
           (actual (clutch--render-row-line 1 render-state)))
      (should (equal-including-properties actual expected)))))

(ert-deftest clutch-test-render-cell-uses-render-state-edits ()
  "Cell rendering should use render-state lookups instead of alist scans."
  (with-temp-buffer
    (setq-local clutch--result-column-defs '((:name "id" :type-category numeric)
                                             (:name "name" :type-category text))
                clutch--row-identity (clutch-test--primary-row-identity
                                      "users" '("id") '(0)))
    (let* ((clutch--pending-edits '((([1] . 1) . "edited")))
           (render-state (clutch--build-render-state))
           (cell (clutch--render-cell '(1 "before") 0 1 [4 8] render-state)))
      (should (string-match-p "edited" cell))
      (should (eq (get-text-property 3 'clutch-col-idx cell) 1))
      (should (equal (get-text-property 3 'clutch-full-value cell) "edited")))))

(ert-deftest clutch-test-render-cell-fk-face-only-covers-displayed-value ()
  "Foreign-key face should not underline trailing cell padding."
  (with-temp-buffer
    (setq-local clutch--result-column-defs '((:name "id" :type-category numeric)
                                             (:name "customer_id" :type-category text))
                clutch--fk-info '((1 :ref-table "customers" :ref-column "id")))
    (let* ((clutch-column-padding 1)
           (cell (clutch--render-cell '(1 "abc") 0 1 [4 6] nil)))
      (should (eq (get-text-property 2 'face cell) 'clutch-fk-face))
      (should (eq (get-text-property 4 'face cell) 'clutch-fk-face))
      (should-not (get-text-property 5 'face cell)))))

(ert-deftest clutch-test-display-select-records-source-table ()
  "SELECT result lifecycle should cache verified source table metadata."
  (let ((result-name "*clutch-test-result*")
        (result (make-clutch-db-result
                 :columns '((:name "id" :type-category numeric))
                 :rows '((1)))))
    (dolist (case '(("SELECT * FROM orders" (:table "orders") nil)
                    ("SELECT * FROM (SELECT * FROM orders) AS _clutch_filter WHERE id = 1"
                     (:table "_clutch_filter")
                     (:server-pageable t :server-rewritable t :source-table "orders"))))
      (clutch-test--with-result-buffer (result-name)
        (pcase-let ((`(,sql ,prep ,context) case))
          (clutch-result--display-select
           'fake-conn sql result 0 prep t context (current-buffer))
          (with-current-buffer result-name
            (should (equal clutch--result-source-table "orders"))))))))

(ert-deftest clutch-test-init-result-state-clears-stale-result-flags ()
  "Result initialization should not keep stale source or DML metadata."
  (with-temp-buffer
    (setq-local clutch--result-source-table "users"
                clutch--result-server-pageable t
                clutch--result-server-rewritable t
                clutch--dml-result t)
    (clutch-result--init-state
     'fake-conn "SELECT name FROM users"
     '((:name "name" :type-category text)) '(("alice")) nil
     nil 0 nil nil nil nil)
    (should-not clutch--result-source-table)
    (should-not clutch--result-server-pageable)
    (should-not clutch--result-server-rewritable)
    (should-not clutch--dml-result)))

(ert-deftest clutch-test-result-source-table-uses-recorded-state ()
  "Result edit paths should use only recorded source table metadata."
  (with-temp-buffer
    (setq-local clutch--result-source-table "orders"
                clutch--last-query "SELECT * FROM stale_table")
    (should (equal (clutch--result-source-table-or-user-error "Stage UPDATE")
                   "orders")))
  (with-temp-buffer
    (setq-local clutch--result-source-table nil
                clutch--last-query "SELECT * FROM users")
    (should-error (clutch--result-source-table-or-user-error "Stage UPDATE")
                  :type 'user-error)))

(ert-deftest clutch-test-insert-data-rows-marks-pending-edits ()
  "Edited rows should show an E marker in the left prefix."
  (with-temp-buffer
    (setq-local clutch--result-columns '("id" "name")
                clutch--result-column-defs '((:name "id" :type-category numeric)
                                             (:name "name" :type-category text))
                clutch--result-rows '((1 "before"))
                clutch--filtered-rows nil
                clutch-result-max-rows 100
                clutch--page-current 0
                clutch--column-widths [4 8]
                clutch--row-identity (clutch-test--primary-row-identity
                                      "users" '("id") '(0))
                clutch--pending-edits '((([1] . 1) . "edited"))
                clutch--pending-deletes nil
                clutch--marked-rows nil)
    (let ((row-positions (make-vector 1 nil)))
      (clutch--insert-data-rows '((1 "before"))
                                row-positions
                                (clutch--build-render-state))
      (should (string-prefix-p "│E  1 " (buffer-string))))))

(ert-deftest clutch-test-record-render-uses-row-identity-keyed-pending-edits ()
  "Record view should render staged edits keyed by row identity."
  (let ((result-buf (generate-new-buffer "*clutch-result*")))
    (unwind-protect
        (progn
          (with-current-buffer result-buf
            (setq-local clutch--result-columns '("id" "name")
                        clutch--result-column-defs '((:name "id" :type-category numeric)
                                                     (:name "name" :type-category text))
                        clutch--result-rows '((1 "before"))
                        clutch--row-identity (clutch-test--primary-row-identity
                                              "users" '("id") '(0))
                        clutch--pending-edits '((([1] . 1) . "edited"))
                        clutch--fk-info nil))
          (with-temp-buffer
            (setq-local clutch-record--result-buffer result-buf
                        clutch-record--row-idx 0
                        clutch-record--expanded-fields nil)
            (clutch-record--render)
            (let ((rendered (buffer-string)))
              (should (string-match-p "edited" rendered))
              (should-not (string-match-p "before" rendered)))))
      (kill-buffer result-buf))))

(ert-deftest clutch-test-record-render-inherits-live-connection-context ()
  "Record view should reuse the parent result buffer connection context."
  (let ((result-buf (generate-new-buffer "*clutch-result*")))
    (unwind-protect
        (progn
          (with-current-buffer result-buf
            (setq-local clutch-connection 'fake-conn
                        clutch--connection-params '(:backend mysql :host "db")
                        clutch--conn-sql-product 'mysql
                        clutch--result-columns '("id" "name")
                        clutch--result-column-defs '((:name "id" :type-category numeric)
                                                     (:name "name" :type-category text))
                        clutch--result-rows '((1 "before"))
                        clutch--pending-edits nil
                        clutch--fk-info nil))
          (with-temp-buffer
            (clutch-record-mode)
            (setq-local clutch-record--result-buffer result-buf
                        clutch-record--row-idx 0
                        clutch-record--expanded-fields nil)
            (clutch-record--render)
            (should (eq clutch-connection 'fake-conn))
            (should (equal clutch--connection-params '(:backend mysql :host "db")))
            (should (eq clutch--conn-sql-product 'mysql))
            (let ((line (clutch--header-with-disconnect-badge
                         clutch-record--header-base)))
              (should-not (string-match-p "Disconnected" line)))))
      (kill-buffer result-buf))))

(ert-deftest clutch-test-record-open-renders-current-row ()
  "Opening the record view should render the row at point."
  (let (record-buf)
    (unwind-protect
        (with-temp-buffer
          (clutch-result-mode)
          (setq-local clutch-connection nil
                      clutch--connection-params nil
                      clutch--conn-sql-product nil
                      clutch--result-columns '("id" "name")
                      clutch--result-column-defs '((:name "id" :type-category numeric)
                                                   (:name "name" :type-category text))
                      clutch--result-rows '((1 "alice") (2 "bob") (3 "carol"))
                      clutch--filtered-rows nil
                      clutch--pending-edits nil
                      clutch--pending-deletes nil
                      clutch--pending-inserts nil
                      clutch--marked-rows nil
                      clutch--sort-column nil
                      clutch--sort-descending nil
                      clutch--page-current 0
                      clutch--page-total-rows 3
                      clutch--fk-info nil
                      clutch--column-widths [2 5])
          (clutch--refresh-display)
          (goto-char (point-min))
          (let ((match (text-property-search-forward 'clutch-row-idx 1 #'eq)))
            (should match)
            (goto-char (prop-match-beginning match)))
          (cl-letf (((symbol-function 'pop-to-buffer)
                     (lambda (buf &rest _args)
                       (setq record-buf buf)
                       buf)))
            (clutch-result-open-record))
          (should (buffer-live-p record-buf))
          (with-current-buffer record-buf
            (let ((rendered (buffer-string)))
              (should (string-match-p "id" rendered))
              (should (string-match-p "name" rendered))
              (should (string-match-p "id\\s-*:\\s-*2" rendered))
              (should (string-match-p "name\\s-*:\\s-*bob" rendered)))))
      (when (buffer-live-p record-buf)
        (kill-buffer record-buf)))))

(ert-deftest clutch-test-record-next-row-advances ()
  "Record view should advance to the next row."
  (let ((result-buf (generate-new-buffer "*clutch-result*")))
    (unwind-protect
        (progn
          (with-current-buffer result-buf
            (setq-local clutch--result-rows '((1 "alice") (2 "bob") (3 "carol"))))
          (with-temp-buffer
            (clutch-record-mode)
            (setq-local clutch-record--result-buffer result-buf
                        clutch-record--row-idx 0
                        clutch-record--expanded-fields nil)
            (cl-letf (((symbol-function 'clutch-record--render) #'ignore))
              (clutch-record-next-row)
              (should (= clutch-record--row-idx 1)))))
      (kill-buffer result-buf))))

(ert-deftest clutch-test-record-next-row-errors-at-last ()
  "Record view should error when already at the last row."
  (let ((result-buf (generate-new-buffer "*clutch-result*")))
    (unwind-protect
        (progn
          (with-current-buffer result-buf
            (setq-local clutch--result-rows '((1 "alice") (2 "bob") (3 "carol"))))
          (with-temp-buffer
            (clutch-record-mode)
            (setq-local clutch-record--result-buffer result-buf
                        clutch-record--row-idx 2
                        clutch-record--expanded-fields nil)
            (should-error (clutch-record-next-row) :type 'user-error)))
      (kill-buffer result-buf))))

(ert-deftest clutch-test-record-prev-row-decrements ()
  "Record view should move back to the previous row."
  (with-temp-buffer
    (clutch-record-mode)
    (setq-local clutch-record--row-idx 2
                clutch-record--expanded-fields nil)
    (cl-letf (((symbol-function 'clutch-record--render) #'ignore))
      (clutch-record-prev-row)
      (should (= clutch-record--row-idx 1)))))

(ert-deftest clutch-test-record-prev-row-errors-at-first ()
  "Record view should error when already at the first row."
  (with-temp-buffer
    (clutch-record-mode)
    (setq-local clutch-record--row-idx 0
                clutch-record--expanded-fields nil)
    (should-error (clutch-record-prev-row) :type 'user-error)))

(ert-deftest clutch-test-record-render-errors-when-result-buffer-dead ()
  "Record render should error when its source result buffer is dead."
  (let ((result-buf (generate-new-buffer "*clutch-result*")))
    (kill-buffer result-buf)
    (with-temp-buffer
      (clutch-record-mode)
      (setq-local clutch-record--result-buffer result-buf
                  clutch-record--row-idx 0
                  clutch-record--expanded-fields nil)
      (should-error (clutch-record--render) :type 'user-error))))

(ert-deftest clutch-test-render-separator ()
  "Test table separator line rendering."
  (let ((visible-cols '(0 1 2))
        (widths [5 10 8]))
    (let ((sep (clutch--render-separator visible-cols widths 'top)))
      (should (stringp sep))
      (should (> (length sep) 0)))))

;;;; Rendering — header-line and footer

(ert-deftest clutch-test-header-line-with-hscroll-matches-body-offset ()
  "Header-line hscroll should track body hscroll exactly."
  (with-temp-buffer
    (setq-local clutch--header-line-string "0123456789")
    (cl-letf (((symbol-function 'window-hscroll) (lambda (&optional _window) 3)))
      (should (equal (clutch--header-line-with-hscroll) "3456789")))))

(ert-deftest clutch-test-header-line-display-prefixes-align-to-zero ()
  "Header-line display should remain aligned to the window's left edge."
  (with-temp-buffer
    (setq-local clutch--header-line-string "abc")
    (cl-letf (((symbol-function 'window-hscroll) (lambda (&optional _window) 0)))
      (let ((rendered (clutch--header-line-display)))
        (should (equal (substring rendered 1) "abc"))
        (should (equal (get-text-property 0 'display rendered)
                       '(space :align-to 0)))))))

(ert-deftest clutch-test-refresh-footer-line-updates-without-changing-body ()
  "Footer refresh should update mode-line state without touching buffer text."
  (with-temp-buffer
    (clutch-test--setup-rendered-result)
    (let ((body (buffer-string))
          (before (substring-no-properties clutch--footer-base-string)))
      (setq-local clutch--page-total-rows 9)
      (clutch--refresh-footer-line)
      (should (equal body (buffer-string)))
      (should-not (equal before
                         (substring-no-properties clutch--footer-base-string)))
      (should (string-match-p "9"
                              (substring-no-properties clutch--footer-base-string))))))

(ert-deftest clutch-test-refresh-header-line-updates-without-changing-body ()
  "Header refresh should update header state without touching buffer text."
  (with-temp-buffer
    (clutch-test--setup-rendered-result)
    (let ((body (buffer-string))
          (before (substring-no-properties clutch--header-line-string)))
      (setq-local clutch--sort-column "name"
                  clutch--sort-descending t)
      (clutch--refresh-header-line)
      (should (equal body (buffer-string)))
      (should-not (equal before
                         (substring-no-properties clutch--header-line-string))))))

(ert-deftest clutch-test-footer-filter-parts-omits-sql-preview ()
  "Footer filter parts should no longer include last SQL preview text."
  (with-temp-buffer
    (setq-local clutch--last-query "SELECT id FROM t")
    (should (equal (clutch--footer-filter-parts) nil))))

(ert-deftest clutch-test-footer-filter-parts-includes-aggregate-summary ()
  "Footer filter parts should include aggregate summary segment."
  (with-temp-buffer
    (setq-local clutch--aggregate-summary
                '(:label "selection" :rows 2 :cells 4 :skipped 0
                         :sum 62 :avg 15.5 :min 10 :max 21 :count 4))
    (let ((parts (clutch--footer-filter-parts)))
      (should (= (length parts) 1))
      (should-not (string-match-p "selection" (car parts)))
      (should (string-match-p "sum=62" (car parts)))
      (should (string-match-p "\\[r2 c4 s0\\]" (car parts))))))

(ert-deftest clutch-test-render-footer-includes-sort-and-pending-summary ()
  "Footer should aggregate sort state and staged changes."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn
                clutch--order-by '("created_at" . "desc")
                clutch--pending-edits '(a)
                clutch--pending-deletes '(b)
                clutch--pending-inserts '(c))
    (cl-letf (((symbol-function 'clutch--tx-header-line-segment)
               (lambda (_conn) "Tx: Manual*")))
      (let ((footer (substring-no-properties
                     (clutch--render-footer 10 0 500 100))))
        (should (string-match-p "Tx: Manual\\*" footer))
        (should (string-match-p "DESC\\[created_at\\]" footer))
        (should (string-match-p "E-1 D-1 I-1" footer))
        (should (string-match-p "C-c C-c" footer))
        (should (string-match-p "C-c C-k" footer))
        (should-not (string-match-p "commit:" footer))
        (should-not (string-match-p "discard:" footer))))))

(ert-deftest clutch-test-render-footer-shows-global-row-range ()
  "Footer should describe the visible global row range instead of page counts."
  (let ((first-page (substring-no-properties
                     (clutch--render-footer 500 0 500 nil nil t)))
        (middle-page (substring-no-properties
                      (clutch--render-footer 500 1 500 nil nil t)))
        (next-last-page (substring-no-properties
                         (clutch--render-footer 78 1 500 nil)))
        (last-window (substring-no-properties
                      (clutch--render-footer 500 1 500 578 78 nil)))
        (empty-page (substring-no-properties
                     (clutch--render-footer 0 0 500 nil))))
    (should (string-match-p (regexp-quote "1-500 of 501+ rows") first-page))
    (should (string-match-p (regexp-quote "501-1000 of 1001+ rows") middle-page))
    (should (string-match-p (regexp-quote "501-578 of 578 rows") next-last-page))
    (should (string-match-p (regexp-quote "79-578 of 578 rows") last-window))
    (should (string-match-p (regexp-quote "0 of 0 rows") empty-page))))

(ert-deftest clutch-test-render-footer-omits-page-count-segment ()
  "Footer should not show misleading page-count segments like 2/1."
  (let ((footer (substring-no-properties
                 (clutch--render-footer 78 1 500 578))))
    (should-not (string-match-p "[0-9]+/[0-9]+" footer))))

(ert-deftest clutch-test-footer-icon-preserves-icon-family ()
  "Footer icons should keep the nerd-icons font family when tinted."
  (cl-letf (((symbol-function 'clutch--icon)
             (lambda (_spec &rest _fallback)
               (propertize "[files]"
                           'face '(:family "Symbols Nerd Font Mono")))))
    (let* ((icon (clutch--footer-icon '(codicon . "nf-cod-files")
                                      "⊞"
                                      'font-lock-keyword-face))
           (face (get-text-property 0 'face icon)))
      (should (equal face
                     '((:family "Symbols Nerd Font Mono")
                       font-lock-keyword-face))))))

(ert-deftest clutch-test-footer-cursor-part-shows-column-position ()
  "Footer cursor segment should include the current column index and total."
  (with-temp-buffer
    (setq-local clutch--result-columns '("id" "amount" "created_at"))
    (insert "amount")
    (put-text-property (point-min) (point-max) 'clutch-row-idx 11)
    (put-text-property (point-min) (point-max) 'clutch-col-idx 1)
    (goto-char (point-min))
    (should (string-match-p "R-12:amount-2/3"
                            (substring-no-properties
                             (clutch--footer-cursor-part))))))

(ert-deftest clutch-test-render-footer-warns-when-row-identity-missing ()
  "Footer should explain when edit/delete are disabled due to missing identity."
  (with-temp-buffer
    (setq-local clutch--result-columns '("id" "name")
                clutch--result-source-table "users"
                clutch--last-query "SELECT * FROM users")
    (let* ((footer-prop (clutch--render-footer 10 0 500 100))
           (footer (substring-no-properties footer-prop))
           (identity-start (string-match "row identity missing" footer)))
      (should (string-match-p "row identity missing" footer))
      (should-not (string-match-p "users" footer))
      (should (string-match-p "E/D off" footer))
      (should identity-start)
      (should (equal (get-text-property identity-start 'face footer-prop)
                     '(:inherit font-lock-warning-face :weight normal))))))

;;;; Filter

(ert-deftest clutch-test-filter-matches-substring-case-insensitive ()
  "Client-side filter should match substrings case-insensitively."
  (with-temp-buffer
    (clutch-result-mode)
    (setq-local clutch--result-columns '("id" "name")
                clutch--result-column-defs '((:name "id" :type-category numeric)
                                             (:name "name" :type-category text))
                clutch--result-rows '((1 "alice") (2 "bob") (3 "carol"))
                clutch--filtered-rows nil
                clutch--pending-edits nil
                clutch--pending-deletes nil
                clutch--pending-inserts nil
                clutch--marked-rows nil
                clutch--sort-column nil
                clutch--sort-descending nil
                clutch--page-current 0
                clutch--page-total-rows 3
                clutch--column-widths [2 5])
    (cl-letf (((symbol-function 'clutch--render-result) #'ignore))
      (clutch-result--apply-filter "ALI")
      (should (equal clutch--filtered-rows '((1 "alice"))))
      (should (equal clutch--filter-pattern "ALI")))))

(ert-deftest clutch-test-filter-matches-formatted-values ()
  "Client-side filter should match non-string values via their display text."
  (with-temp-buffer
    (clutch-result-mode)
    (setq-local clutch--result-columns '("id" "value")
                clutch--result-column-defs '((:name "id" :type-category numeric)
                                             (:name "value" :type-category numeric))
                clutch--result-rows '((1 nil) (2 42) (3 "hello"))
                clutch--filtered-rows nil
                clutch--pending-edits nil
                clutch--pending-deletes nil
                clutch--pending-inserts nil
                clutch--marked-rows nil
                clutch--sort-column nil
                clutch--sort-descending nil
                clutch--page-current 0
                clutch--page-total-rows 3
                clutch--column-widths [2 5])
    (cl-letf (((symbol-function 'clutch--render-result) #'ignore))
      (clutch-result--apply-filter "42")
      (should (equal clutch--filtered-rows '((2 42))))
      (should (equal clutch--filter-pattern "42")))))

(ert-deftest clutch-test-filter-clear-restores-all-rows ()
  "Clearing the client-side filter should restore the full result set."
  (with-temp-buffer
    (clutch-result-mode)
    (setq-local clutch--result-columns '("id" "name")
                clutch--result-column-defs '((:name "id" :type-category numeric)
                                             (:name "name" :type-category text))
                clutch--result-rows '((1 "alice") (2 "bob") (3 "carol"))
                clutch--filtered-rows nil
                clutch--filter-pattern nil
                clutch--pending-edits nil
                clutch--pending-deletes nil
                clutch--pending-inserts nil
                clutch--marked-rows nil
                clutch--sort-column nil
                clutch--sort-descending nil
                clutch--page-current 0
                clutch--page-total-rows 3
                clutch--column-widths [2 5])
    (cl-letf (((symbol-function 'clutch--render-result) #'ignore))
      (clutch-result--apply-filter "ali")
      (cl-letf (((symbol-function 'read-string) (lambda (&rest _args) "")))
        (clutch-result-filter))
      (should-not clutch--filtered-rows)
      (should-not clutch--filter-pattern))))

(ert-deftest clutch-test-filter-clears-marked-rows ()
  "Applying the client-side filter should clear marked rows."
  (with-temp-buffer
    (clutch-result-mode)
    (setq-local clutch--result-columns '("id" "name")
                clutch--result-column-defs '((:name "id" :type-category numeric)
                                             (:name "name" :type-category text))
                clutch--result-rows '((1 "alice") (2 "bob") (3 "carol"))
                clutch--filtered-rows nil
                clutch--pending-edits nil
                clutch--pending-deletes nil
                clutch--pending-inserts nil
                clutch--marked-rows '(0 2)
                clutch--sort-column nil
                clutch--sort-descending nil
                clutch--page-current 0
                clutch--page-total-rows 3
                clutch--column-widths [2 5])
    (cl-letf (((symbol-function 'clutch--render-result) #'ignore))
      (clutch-result--apply-filter "ali")
      (should-not clutch--marked-rows))))

(ert-deftest clutch-test-apply-filter-errors-without-query ()
  "WHERE filtering should error when there is no last query."
  (with-temp-buffer
    (should-error (clutch-result-apply-filter) :type 'user-error)))

(ert-deftest clutch-test-apply-filter-builds-where-and-executes ()
  "WHERE filtering should build a clause and execute the filtered query."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn
                clutch--last-query "SELECT * FROM t"
                clutch--result-source-table "t"
                clutch--result-server-pageable t
                clutch--result-server-rewritable t
                clutch--result-columns '("id" "name")
                clutch--row-identity
                (clutch-test--primary-row-identity "t" '("id") '(0))
                clutch--where-filter nil)
    (let (captured)
      (cl-letf (((symbol-function 'completing-read) (lambda (&rest _args) "id"))
                ((symbol-function 'read-string) (lambda (&rest _args) "> 5"))
                ((symbol-function 'clutch-db-escape-identifier)
                 (lambda (_conn id) (format "`%s`" id)))
                ((symbol-function 'clutch--execute)
                 (lambda (sql conn &optional result-context)
                   (setq captured
                         (list sql conn
                               (plist-get result-context :source-table)
                               (plist-get
                                (plist-get result-context :row-identity-prep)
                                :sql))))))
        (clutch-result-apply-filter)
        (should (equal captured
                       (list (clutch-db-apply-where
                              'fake-conn "SELECT * FROM t" "`id` > 5")
                             'fake-conn
                             "t"
                             (clutch-db-apply-where
                              'fake-conn
                              "SELECT t.*, `id` AS `clutch__rid_0` FROM t"
                              "`id` > 5"))))
        (should (equal clutch--where-filter "`id` > 5"))
        (should (equal clutch--base-query "SELECT * FROM t"))))))

(ert-deftest clutch-test-apply-filter-defaults-to-visible-active-column ()
  "WHERE filtering should translate active column indices through visible columns."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn
                clutch--last-query "SELECT * FROM t"
                clutch--result-server-pageable t
                clutch--result-server-rewritable t
                clutch--result-columns '("clutch__rid_0" "id" "name")
                clutch--result-column-defs
                '((:name "clutch__rid_0" :hidden t)
                  (:name "id")
                  (:name "name"))
                clutch--header-active-col 1
                clutch--where-filter nil)
    (let (seen)
      (cl-letf (((symbol-function 'clutch--read-where-filter)
                 (lambda (_current columns default-col &optional _conn)
                   (setq seen (list columns default-col))
                   "id > 1"))
                ((symbol-function 'clutch--execute) #'ignore))
        (clutch-result-apply-filter)
        (should (equal seen '(("id" "name") "id")))))))

(ert-deftest clutch-test-apply-filter-empty-condition-clears-filter ()
  "Clearing a WHERE filter should reset the stored clause."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn
                clutch--last-query "SELECT * FROM t"
                clutch--base-query "SELECT * FROM t"
                clutch--result-source-table "t"
                clutch--result-server-pageable t
                clutch--result-server-rewritable t
                clutch--where-filter "id > 5")
    (let (captured)
      (cl-letf (((symbol-function 'read-string) (lambda (&rest _args) ""))
                ((symbol-function 'clutch--execute)
                 (lambda (sql conn &optional _result-context)
                   (setq captured (list sql conn)))))
        (clutch-result-apply-filter)
        (should (equal captured '("SELECT * FROM t" fake-conn)))
        (should-not clutch--where-filter)
        (should-not clutch--base-query)))))

(ert-deftest clutch-test-apply-filter-errors-for-nonrewritable-query-result ()
  "Server-side WHERE filtering should not rewrite arbitrary query results."
  (with-temp-buffer
    (setq-local clutch--result-server-rewritable nil
                clutch-connection 'fake-conn
                clutch--last-query "SELECT a.*, b.* FROM a JOIN b ON a.id = b.id LIMIT 10"
                clutch--base-query clutch--last-query
                clutch--result-columns '("id" "name" "id"))
    (let (executed)
      (cl-letf (((symbol-function 'completing-read) (lambda (&rest _args) "id"))
                ((symbol-function 'read-string) (lambda (&rest _args) "= 1"))
                ((symbol-function 'clutch--execute)
                 (lambda (&rest _args) (setq executed t))))
        (let ((err (should-error (clutch-result-apply-filter)
                                 :type 'user-error)))
          (should (string-match-p "Server-side filter"
                                  (error-message-string err))))
        (should-not executed)))))

;;;; Rendering — custom column displayers

(ert-deftest clutch-test-register-column-displayer-replaces-and-unregisters ()
  "Column displayer registration should replace existing entries and unregister cleanly."
  (let ((clutch-column-displayers nil)
        (first (lambda (_value) "first"))
        (second (lambda (_value) "second")))
    (clutch-register-column-displayer "Orders" "Status" first)
    (should (eq (clutch--lookup-column-displayer "orders" "status") first))
    (clutch-register-column-displayer "orders" "status" second)
    (should (= (length clutch-column-displayers) 1))
    (should (= (length (cdar clutch-column-displayers)) 1))
    (should (eq (clutch--lookup-column-displayer "ORDERS" "STATUS") second))
    (clutch-unregister-column-displayer "ORDERS" "STATUS")
    (should-not clutch-column-displayers)))

(ert-deftest clutch-test-cell-display-content-uses-case-insensitive-column-displayer ()
  "Custom column displayers should match table and column names case-insensitively."
  (let ((clutch-column-displayers nil))
    (clutch-register-column-displayer
     "Orders" "Status"
     (lambda (value)
       (format "state:%s" value)))
    (with-temp-buffer
      (setq-local clutch--result-source-table "orders")
      (should (equal
               (clutch--cell-display-content
                7 12 '(:name "STATUS" :type-category numeric) nil)
               "state:7")))))

(ert-deftest clutch-test-cell-display-content-falls-back-when-displayer-does-not-render ()
  "Nil or errors from a column displayer should fall back to default rendering."
  (dolist (case '(nil-result error-result))
    (ert-info ((format "case: %s" case))
      (let ((clutch-column-displayers nil)
            logged)
        (clutch-register-column-displayer
         "orders" "status"
         (lambda (_value)
           (pcase case
             ('nil-result nil)
             ('error-result (error "Boom")))))
        (with-temp-buffer
          (setq-local clutch--result-source-table "orders")
          (cl-letf (((symbol-function 'message)
                     (lambda (fmt &rest args)
                       (setq logged (apply #'format fmt args)))))
            (should (equal
                     (clutch--cell-display-content
                      "queued" 12 '(:name "status" :type-category text) nil)
                     "queued"))
            (when (eq case 'error-result)
              (should (string-match-p "failed: boom" logged)))))))))

(ert-deftest clutch-test-cell-display-content-truncates-custom-displayer-output ()
  "Custom column displayer output should still be truncated to column width."
  (let ((clutch-column-displayers nil))
    (clutch-register-column-displayer
     "orders" "status"
     (lambda (_value)
       "abcdef"))
    (with-temp-buffer
      (setq-local clutch--result-source-table "orders")
      (should (equal
               (clutch--cell-display-content
                "queued" 4 '(:name "status" :type-category text) nil)
               "abcd")))))

(ert-deftest clutch-test-render-cell-custom-displayer-keeps-full-value-raw ()
  "Custom cell display should keep `clutch-full-value' on the raw value."
  (let ((clutch-column-displayers nil))
    (clutch-register-column-displayer
     "tasks" "status"
     (lambda (_value)
       "done"))
    (with-temp-buffer
      (setq-local clutch--result-source-table "tasks"
                  clutch--result-column-defs
                  '((:name "status" :type-category numeric)))
      (let ((cell (clutch--render-cell '(2) 0 0 [6] nil)))
        (should (string-match-p "done" cell))
        (should (= (get-text-property 2 'clutch-full-value cell) 2))))))

;;;; Rendering — live value viewer

(defun clutch-test--prepare-live-view-source (buffer)
  "Populate BUFFER as a simple result grid for live-view tests."
  (with-current-buffer buffer
    (clutch-result-mode)
    (setq-local clutch--result-source-table "cases")
    (setq-local clutch--result-columns '("id" "name"))
    (setq-local clutch--result-column-defs
                '((:name "id" :type-category numeric)
                  (:name "name" :type-category text)))
    (setq-local clutch--result-rows '((1 "alice") (2 "bob")))
    (setq-local clutch--filtered-rows nil)
    (setq-local clutch--pending-edits nil)
    (setq-local clutch--pending-deletes nil)
    (setq-local clutch--pending-inserts nil)
    (setq-local clutch--marked-rows nil)
    (setq-local clutch--sort-column nil)
    (setq-local clutch--sort-descending nil)
    (setq-local clutch--page-current 0)
    (setq-local clutch--page-total-rows 2)
    (setq-local clutch--column-widths [2 5])
    (clutch--refresh-display)
    (goto-char (point-min))
    (when-let* ((match (text-property-search-forward 'clutch-col-idx 1 #'eq)))
      (goto-char (prop-match-beginning match)))))

(ert-deftest clutch-test-live-view-follows-result-point ()
  "Live viewer should refresh as point moves across result cells."
  (let ((source (generate-new-buffer " *clutch-live-source*"))
        viewer)
    (unwind-protect
        (progn
          (clutch-test--prepare-live-view-source source)
          (with-current-buffer source
            (cl-letf (((symbol-function 'display-buffer)
                       (lambda (buf &rest _args)
                         (setq viewer buf)
                         buf)))
              (clutch-result-live-view-value)
              (should (buffer-live-p viewer))
              (with-current-buffer viewer
                (should (string-match-p "alice" (buffer-string))))
              (clutch-result-down-cell)
              (run-hooks 'post-command-hook)
              (with-current-buffer viewer
                (should (string-match-p "bob" (buffer-string)))))))
      (when (buffer-live-p viewer)
        (kill-buffer viewer))
      (when (buffer-live-p source)
        (kill-buffer source)))))

(ert-deftest clutch-test-live-view-freeze-stops-following ()
  "Frozen live viewer should ignore subsequent source point changes."
  (let ((source (generate-new-buffer " *clutch-live-freeze*"))
        viewer)
    (unwind-protect
        (progn
          (clutch-test--prepare-live-view-source source)
          (with-current-buffer source
            (cl-letf (((symbol-function 'display-buffer)
                       (lambda (buf &rest _args)
                         (setq viewer buf)
                         buf)))
              (clutch-result-live-view-value)))
          (with-current-buffer viewer
            (clutch--live-view-toggle-freeze))
          (with-current-buffer source
            (clutch-result-down-cell)
            (run-hooks 'post-command-hook))
          (with-current-buffer viewer
            (should (string-match-p "alice" (buffer-string)))
            (clutch--live-view-toggle-freeze)
            (should (string-match-p "bob" (buffer-string)))))
      (when (buffer-live-p viewer)
        (kill-buffer viewer))
      (when (buffer-live-p source)
        (kill-buffer source)))))

(ert-deftest clutch-test-live-view-quit-detaches-source-hooks ()
  "Closing the live viewer should detach it from the source buffer."
  (let ((source (generate-new-buffer " *clutch-live-quit*"))
        viewer)
    (unwind-protect
        (progn
          (clutch-test--prepare-live-view-source source)
          (with-current-buffer source
            (cl-letf (((symbol-function 'display-buffer)
                       (lambda (buf &rest _args)
                         (setq viewer buf)
                         buf)))
              (clutch-result-live-view-value)
              (should (eq clutch--live-view-buffer viewer))
              (should (memq #'clutch--live-view-source-post-command
                            post-command-hook))))
          (with-current-buffer viewer
            (clutch--live-view-quit))
          (should-not (buffer-live-p viewer))
          (with-current-buffer source
            (should-not clutch--live-view-buffer)
            (should-not (memq #'clutch--live-view-source-post-command
                              post-command-hook))))
      (when (buffer-live-p viewer)
        (kill-buffer viewer))
      (when (buffer-live-p source)
        (kill-buffer source)))))

;;;; Shell command on cell

(ert-deftest clutch-test-shell-command-on-cell-pipes-formatted-value ()
  "Shell commands should receive the current cell value as formatted stdin."
  (dolist (case '(("hello world" "hello world")
                  (42 "42")))
    (pcase-let ((`(,value ,expected) case))
      (ert-info ((format "value: %S" value))
        (with-temp-buffer
          (let (captured)
            (cl-letf (((symbol-function 'clutch--cell-at-point)
                       (lambda () (list 0 1 value)))
                      ((symbol-function 'clutch--view-in-buffer)
                       (lambda (val &rest _args)
                         (setq captured val))))
              (clutch-result-shell-command-on-cell "cat")
              (should (string-match-p expected captured)))))))))

(ert-deftest clutch-test-shell-command-on-cell-errors-without-cell ()
  "Shell commands should error when point is not on a result cell."
  (with-temp-buffer
    (should-error (clutch-result-shell-command-on-cell "cat") :type 'user-error)))

;;;; Schema cache — refresh and status

(ert-deftest clutch-test-refresh-schema-cache-records-ready-status ()
  "Schema refresh should record ready state and table count."
  (let ((clutch--schema-cache (make-hash-table :test 'equal))
        (clutch--column-details-cache (make-hash-table :test 'equal))
        (clutch--table-comment-cache (make-hash-table :test 'equal))
        (clutch--help-doc-cache (make-hash-table :test 'equal))
        (clutch--schema-status-cache (make-hash-table :test 'equal)))
    (cl-letf (((symbol-function 'clutch-db-list-tables)
               (lambda (_conn) '("users" "orders")))
              ((symbol-function 'clutch--connection-key)
               (lambda (_conn) "fake"))
              ((symbol-function 'clutch-db-live-p)
               (lambda (_conn) t))
              ((symbol-function 'clutch--refresh-schema-status-ui) #'ignore))
      (should (clutch--refresh-schema-cache 'fake-conn))
      (should (eq (plist-get (gethash "fake" clutch--schema-status-cache) :state)
                  'ready))
      (should (= (plist-get (gethash "fake" clutch--schema-status-cache) :tables)
                 2)))))

(ert-deftest clutch-test-refresh-schema-cache-async-records-ready-status ()
  "Async schema refresh should record ready state and table count."
  (let ((clutch--schema-cache (make-hash-table :test 'equal))
        (clutch--column-details-cache (make-hash-table :test 'equal))
        (clutch--table-comment-cache (make-hash-table :test 'equal))
        (clutch--help-doc-cache (make-hash-table :test 'equal))
        (clutch--schema-status-cache (make-hash-table :test 'equal))
        (clutch--schema-refresh-tickets (make-hash-table :test 'equal))
        (clutch--schema-refresh-ticket-counter 0))
    (cl-letf (((symbol-function 'clutch--connection-key)
               (lambda (_conn) "fake"))
              ((symbol-function 'clutch-db-live-p)
               (lambda (_conn) t))
              ((symbol-function 'clutch--refresh-schema-status-ui) #'ignore)
              ((symbol-function 'clutch-db-refresh-schema-async)
               (lambda (_conn callback &optional _errback)
                 (funcall callback '("users" "orders"))
                 t)))
      (should (clutch--refresh-schema-cache-async 'fake-conn))
      (should (eq (plist-get (gethash "fake" clutch--schema-status-cache) :state)
                  'ready))
      (should (= (plist-get (gethash "fake" clutch--schema-status-cache) :tables)
                 2)))))

(ert-deftest clutch-test-refresh-schema-cache-async-ignores-stale-callbacks ()
  "Old async schema refresh callbacks should not overwrite newer state."
  (let ((clutch--schema-cache (make-hash-table :test 'equal))
        (clutch--column-details-cache (make-hash-table :test 'equal))
        (clutch--table-comment-cache (make-hash-table :test 'equal))
        (clutch--help-doc-cache (make-hash-table :test 'equal))
        (clutch--schema-status-cache (make-hash-table :test 'equal))
        (clutch--schema-refresh-tickets (make-hash-table :test 'equal))
        (clutch--schema-refresh-ticket-counter 0)
        first-callback second-callback)
    (cl-letf (((symbol-function 'clutch--connection-key)
               (lambda (_conn) "fake"))
              ((symbol-function 'clutch-db-live-p)
               (lambda (_conn) t))
              ((symbol-function 'clutch--refresh-schema-status-ui) #'ignore)
              ((symbol-function 'clutch-db-refresh-schema-async)
               (lambda (_conn callback &optional _errback)
                 (if first-callback
                     (setq second-callback callback)
                   (setq first-callback callback))
                 t)))
      (should (clutch--refresh-schema-cache-async 'fake-conn))
      (should (clutch--refresh-schema-cache-async 'fake-conn))
      (funcall first-callback '("stale_users"))
      (should-not (gethash "fake" clutch--schema-cache))
      (funcall second-callback '("users" "orders"))
      (should (= (hash-table-count (gethash "fake" clutch--schema-cache)) 2))
      (should (eq (plist-get (gethash "fake" clutch--schema-status-cache) :state)
                  'ready)))))

(ert-deftest clutch-test-refresh-schema-cache-async-records-debug-submit-success-and-stale ()
  "Async schema refresh should record submit, success, and stale-drop events."
  (let ((clutch--schema-cache (make-hash-table :test 'equal))
        (clutch--column-details-cache (make-hash-table :test 'equal))
        (clutch--table-comment-cache (make-hash-table :test 'equal))
        (clutch--help-doc-cache (make-hash-table :test 'equal))
        (clutch--schema-status-cache (make-hash-table :test 'equal))
        (clutch--schema-refresh-tickets (make-hash-table :test 'equal))
        (clutch--schema-refresh-ticket-counter 0)
        first-callback second-callback)
    (with-temp-buffer
      (let ((clutch-debug-mode t))
        (setq-local clutch-connection 'fake-conn)
        (cl-letf (((symbol-function 'clutch--connection-key)
                   (lambda (_conn) "fake"))
                  ((symbol-function 'clutch-db-live-p)
                   (lambda (_conn) t))
                  ((symbol-function 'clutch--connection-alive-p)
                   (lambda (_conn) t))
                  ((symbol-function 'clutch--backend-key-from-conn)
                   (lambda (_conn) 'mysql))
                  ((symbol-function 'clutch--refresh-schema-status-ui) #'ignore)
                  ((symbol-function 'clutch--invalidate-object-warmup) #'ignore)
                  ((symbol-function 'clutch--schedule-object-warmup) #'ignore)
                  ((symbol-function 'clutch-db-refresh-schema-async)
                   (lambda (_conn callback &optional _errback)
                     (if first-callback
                         (setq second-callback callback)
                       (setq first-callback callback))
                     t))
                  ((symbol-function 'pop-to-buffer)
                   (lambda (buf &rest _args) buf)))
          (clutch--clear-debug-capture)
          (should (clutch--refresh-schema-cache-async 'fake-conn))
          (should (clutch--refresh-schema-cache-async 'fake-conn))
          (funcall first-callback '("stale_users"))
          (funcall second-callback '("users" "orders"))
          (let ((text (clutch-test--debug-buffer-string)))
            (should (string-match-p "Operation: schema-refresh" text))
            (should (string-match-p "Phase: submit" text))
            (should (string-match-p "Phase: success" text))
            (should (string-match-p "Phase: stale-drop" text))))))))

(ert-deftest clutch-test-install-schema-cache-batches-large-refreshes ()
  "Large schema installs should be split across idle slices."
  (let ((clutch--schema-cache (make-hash-table :test 'equal))
        (clutch--column-details-cache (make-hash-table :test 'equal))
        (clutch--column-details-status-cache (make-hash-table :test 'equal))
        (clutch--column-details-queue-cache (make-hash-table :test 'equal))
        (clutch--column-details-active-cache (make-hash-table :test 'equal))
        (clutch--columns-status-cache (make-hash-table :test 'equal))
        (clutch--table-comment-cache (make-hash-table :test 'equal))
        (clutch--help-doc-cache (make-hash-table :test 'equal))
        (clutch--object-cache (make-hash-table :test 'equal))
        (clutch--object-warmup-timers (make-hash-table :test 'equal))
        (clutch--schema-status-cache (make-hash-table :test 'equal))
        (clutch--schema-refresh-tickets (make-hash-table :test 'equal))
        (clutch--schema-install-timers (make-hash-table :test 'equal))
        (clutch-schema-cache-install-batch-size 2)
        callbacks
        warmup-called)
    (cl-letf (((symbol-function 'clutch--connection-key)
               (lambda (_conn) "fake"))
              ((symbol-function 'clutch-db-live-p)
               (lambda (_conn) t))
              ((symbol-function 'clutch--refresh-schema-status-ui) #'ignore)
              ((symbol-function 'clutch--schedule-object-warmup)
               (lambda (_conn) (setq warmup-called t)))
              ((symbol-function 'run-with-idle-timer)
               (lambda (_secs _repeat fn)
                 (push fn callbacks)
                 (intern (format "fake-timer-%s" (length callbacks))))))
      (puthash "fake" 1 clutch--schema-refresh-tickets)
      (puthash "fake" '(:state refreshing) clutch--schema-status-cache)
      (should (clutch--install-schema-cache 'fake-conn '("a" "b" "c") 1))
      (should-not (gethash "fake" clutch--schema-cache))
      (should (eq (plist-get (gethash "fake" clutch--schema-status-cache) :state) 'refreshing))
      (funcall (car (last callbacks)))
      (should-not (gethash "fake" clutch--schema-cache))
      (funcall (car callbacks))
      (should (= (hash-table-count (gethash "fake" clutch--schema-cache)) 3))
      (should (eq (plist-get (gethash "fake" clutch--schema-status-cache) :state) 'ready))
      (should warmup-called))))

(ert-deftest clutch-test-console-buffer-name-reflects-schema-status ()
  "Console buffer names should expose schema status."
  (let ((clutch--schema-status-cache (make-hash-table :test 'equal)))
    (with-temp-buffer
      (clutch-mode)
      (setq-local clutch--console-name "dev"
                  clutch-connection 'fake-conn)
      (cl-letf (((symbol-function 'clutch--connection-key)
                 (lambda (_conn) "dev-key")))
        (puthash "dev-key" '(:state stale) clutch--schema-status-cache)
        (clutch--update-console-buffer-name)
        (should (equal (buffer-name) "*clutch: dev* [schema~]"))
        (puthash "dev-key" '(:state refreshing) clutch--schema-status-cache)
        (clutch--update-console-buffer-name)
        (should (equal (buffer-name) "*clutch: dev* [schema...]"))
        (puthash "dev-key" '(:state ready :tables 42) clutch--schema-status-cache)
        (clutch--update-console-buffer-name)
        (should (equal (buffer-name) "*clutch: dev* [schema 42t]"))))))

(ert-deftest clutch-test-schema-status-header-line-segment ()
  "Schema states should produce the correct header-line segment text."
  (let ((clutch--schema-status-cache (make-hash-table :test 'equal)))
    (cl-letf (((symbol-function 'clutch--connection-key)
               (lambda (_conn) "dev-key")))
      (puthash "dev-key" '(:state stale) clutch--schema-status-cache)
      (should (equal (clutch--schema-status-header-line-segment 'fake-conn)
                     (propertize "schema~" 'face 'warning)))
      (puthash "dev-key" '(:state failed) clutch--schema-status-cache)
      (should (equal (clutch--schema-status-header-line-segment 'fake-conn)
                     (propertize "schema!" 'face 'error)))
      (puthash "dev-key" '(:state refreshing) clutch--schema-status-cache)
      (should (equal (clutch--schema-status-header-line-segment 'fake-conn)
                     (propertize "schema…" 'face 'shadow))))))

(ert-deftest clutch-test-schema-cache-guidance-reflects-recovery-state ()
  "Schema cache guidance should explain how to recover from non-ready states."
  (let ((clutch--schema-status-cache (make-hash-table :test 'equal)))
    (cl-letf (((symbol-function 'clutch--connection-key)
               (lambda (_conn) "dev-key")))
      (puthash "dev-key" '(:state stale) clutch--schema-status-cache)
      (should (string-match-p "C-c C-s"
                              (clutch--schema-cache-guidance 'fake-conn)))
      (puthash "dev-key" '(:state failed) clutch--schema-status-cache)
      (should (string-match-p "retry"
                              (clutch--schema-cache-guidance 'fake-conn)))
      (puthash "dev-key" '(:state refreshing) clutch--schema-status-cache)
      (should (string-match-p "in progress"
                              (clutch--schema-cache-guidance 'fake-conn)))
      (puthash "dev-key" '(:state ready :tables 2) clutch--schema-status-cache)
      (should-not (clutch--schema-cache-guidance 'fake-conn)))))

(ert-deftest clutch-test-refresh-current-schema-avoids-duplicate-refresh ()
  "Refreshing state should block duplicate schema refresh attempts."
  (let ((clutch--schema-status-cache (make-hash-table :test 'equal))
        seen-message
        refresh-called)
    (cl-letf (((symbol-function 'clutch--connection-key)
               (lambda (_conn) "dev-key"))
              ((symbol-function 'clutch--ensure-connection) #'ignore)
              ((symbol-function 'clutch--refresh-schema-cache)
               (lambda (_conn) (setq refresh-called t) t))
              ((symbol-function 'message)
               (lambda (fmt &rest args)
                 (setq seen-message (apply #'format fmt args)))))
      (puthash "dev-key" '(:state refreshing) clutch--schema-status-cache)
      (with-temp-buffer
        (setq-local clutch-connection 'fake-conn)
        (should-not (clutch--refresh-current-schema))
        (should-not refresh-called)
        (should (string-match-p "already in progress" seen-message))))))

(ert-deftest clutch-test-refresh-current-schema-starts-background-refresh-for-lazy-backends ()
  "Lazy backends should start schema refresh in the background."
  (let ((clutch--schema-status-cache (make-hash-table :test 'equal))
        seen-message
        sync-called
        async-called)
    (cl-letf (((symbol-function 'clutch--connection-key)
               (lambda (_conn) "dev-key"))
              ((symbol-function 'clutch--ensure-connection) #'ignore)
              ((symbol-function 'clutch-db-eager-schema-refresh-p)
               (lambda (_conn) nil))
              ((symbol-function 'clutch--refresh-schema-cache)
               (lambda (_conn) (setq sync-called t) t))
              ((symbol-function 'clutch--refresh-schema-cache-async)
               (lambda (_conn) (setq async-called t) t))
              ((symbol-function 'message)
               (lambda (fmt &rest args)
                 (setq seen-message (apply #'format fmt args)))))
      (with-temp-buffer
        (setq-local clutch-connection 'fake-conn)
        (should (clutch--refresh-current-schema))
        (should async-called)
        (should-not sync-called)
        (should (string-match-p "started in background" seen-message))))))

(ert-deftest clutch-test-prime-schema-cache-falls-back-to-sync-when-async-unavailable ()
  "Schema priming should fall back to sync refresh when async is unavailable."
  (let (sync-called async-called)
    (cl-letf (((symbol-function 'clutch-db-eager-schema-refresh-p)
               (lambda (_conn) nil))
              ((symbol-function 'clutch--refresh-schema-cache-async)
               (lambda (_conn)
                 (setq async-called t)
                 nil))
              ((symbol-function 'clutch--refresh-schema-cache)
               (lambda (_conn)
                 (setq sync-called t)
                 t)))
      (clutch--prime-schema-cache 'fake-conn)
      (should async-called)
      (should sync-called))))

(ert-deftest clutch-test-refresh-current-schema-falls-back-to-sync-when-background-refresh-is-unavailable ()
  "Manual schema refresh should fall back to sync when async setup is unavailable."
  (let ((clutch--schema-status-cache (make-hash-table :test 'equal))
        seen-message
        sync-called
        async-called)
    (cl-letf (((symbol-function 'clutch--connection-key)
               (lambda (_conn) "dev-key"))
              ((symbol-function 'clutch--ensure-connection) #'ignore)
              ((symbol-function 'clutch-db-eager-schema-refresh-p)
               (lambda (_conn) nil))
              ((symbol-function 'clutch--refresh-schema-cache-async)
               (lambda (_conn)
                 (setq async-called t)
                 nil))
              ((symbol-function 'clutch--refresh-schema-cache)
               (lambda (_conn)
                 (setq sync-called t)
                 (puthash "dev-key" '(:state ready :tables 2)
                          clutch--schema-status-cache)
                 t))
              ((symbol-function 'message)
               (lambda (fmt &rest args)
                 (setq seen-message (apply #'format fmt args)))))
      (with-temp-buffer
        (setq-local clutch-connection 'fake-conn)
        (should (clutch--refresh-current-schema))
        (should async-called)
        (should sync-called)
        (should (string-match-p (regexp-quote "Schema refreshed (2 tables)")
                                seen-message))))))

(ert-deftest clutch-test-refresh-schema-command-forces-sync-refresh-on-lazy-backends ()
  "Explicit schema refresh should bypass background refresh for lazy backends."
  (let ((clutch--schema-status-cache (make-hash-table :test 'equal))
        seen-message
        sync-called
        async-called)
    (cl-letf (((symbol-function 'clutch--connection-key)
               (lambda (_conn) "dev-key"))
              ((symbol-function 'clutch--ensure-connection) #'ignore)
              ((symbol-function 'clutch-db-eager-schema-refresh-p)
               (lambda (_conn) nil))
              ((symbol-function 'clutch--refresh-schema-cache-async)
               (lambda (_conn)
                 (setq async-called t)
                 t))
              ((symbol-function 'clutch--refresh-schema-cache)
               (lambda (_conn)
                 (setq sync-called t)
                 (puthash "dev-key" '(:state ready :tables 2)
                          clutch--schema-status-cache)
                 t))
              ((symbol-function 'message)
               (lambda (fmt &rest args)
                 (setq seen-message (apply #'format fmt args)))))
      (with-temp-buffer
        (setq-local clutch-connection 'fake-conn)
        (should (clutch-refresh-schema))
        (should sync-called)
        (should-not async-called)
        (should (string-match-p (regexp-quote "Schema refreshed (2 tables)")
                                seen-message))))))

(ert-deftest clutch-test-clear-connection-metadata-caches-cancels-explicit-key-timers ()
  "Clearing metadata for an explicit key should cancel timers for that key."
  (let ((clutch--schema-cache (make-hash-table :test 'equal))
        (clutch--columns-status-cache (make-hash-table :test 'equal))
        (clutch--column-details-cache (make-hash-table :test 'equal))
        (clutch--column-details-status-cache (make-hash-table :test 'equal))
        (clutch--column-details-queue-cache (make-hash-table :test 'equal))
        (clutch--column-details-active-cache (make-hash-table :test 'equal))
        (clutch--table-comment-cache (make-hash-table :test 'equal))
        (clutch--help-doc-cache (make-hash-table :test 'equal))
        (clutch--object-cache (make-hash-table :test 'equal))
        (clutch--schema-status-cache (make-hash-table :test 'equal))
        (clutch--schema-refresh-tickets (make-hash-table :test 'equal))
        (clutch--schema-install-timers (make-hash-table :test 'equal))
        (clutch--object-warmup-timers (make-hash-table :test 'equal))
        canceled)
    (puthash "old-key" 'schema-timer clutch--schema-install-timers)
    (puthash "old-key" 'warmup-timer clutch--object-warmup-timers)
    (cl-letf (((symbol-function 'cancel-timer)
               (lambda (timer)
                 (push timer canceled)))
              ((symbol-function 'clutch--connection-key)
               (lambda (_conn) "new-key"))
              ((symbol-function 'clutch--object-cache-key)
               (lambda (_conn) "new-key")))
      (clutch--clear-connection-metadata-caches 'fake-conn "old-key")
      (should (member 'schema-timer canceled))
      (should (member 'warmup-timer canceled))
      (should-not (gethash "old-key" clutch--schema-install-timers))
      (should-not (gethash "old-key" clutch--object-warmup-timers)))))

(ert-deftest clutch-test-describe-table-warns-when-schema-cache-is-stale ()
  "Cache-backed table prompts should surface stale-schema recovery hints."
  (let ((clutch--schema-status-cache (make-hash-table :test 'equal))
        hinted
        described)
    (cl-letf (((symbol-function 'clutch--connection-key)
               (lambda (_conn) "dev-key"))
              ((symbol-function 'clutch--schema-for-connection)
               (lambda ()
                 (let ((h (make-hash-table :test 'equal)))
                   (puthash "users" nil h)
                   h)))
              ((symbol-function 'clutch--read-table-name)
               (lambda (_prompt _tables) "users"))
              ((symbol-function 'clutch--ensure-connection) #'ignore)
              ((symbol-function 'clutch-object-describe)
               (lambda (entry)
                 (setq described entry)))
              ((symbol-function 'message)
               (lambda (fmt &rest args)
                 (setq hinted (apply #'format fmt args)))))
      (puthash "dev-key" '(:state stale) clutch--schema-status-cache)
      (with-temp-buffer
        (setq-local clutch-connection 'fake-conn)
        (call-interactively #'clutch-describe-table))
      (should (equal described '(:name "users" :type "TABLE")))
      (should (string-match-p "Schema cache is stale" hinted)))))

;;;; Schema cache — column details and metadata

(ert-deftest clutch-test-edit-column-detail-propagates-metadata-errors ()
  "Edit metadata lookup should not silently hide column-detail failures."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn
                clutch--result-source-table "shipping_incidents")
    (cl-letf (((symbol-function 'clutch--ensure-column-details)
               (lambda (&rest _args)
                 (signal 'clutch-db-error '("column detail boom")))))
      (should-error (clutch-result--column-detail (current-buffer) "severity")
                    :type 'clutch-db-error))))

(ert-deftest clutch-test-result-column-info-works-on-cell-padding ()
  "Column info should resolve from padded whitespace inside a data cell."
  (with-temp-buffer
    (setq-local clutch-column-padding 1
                clutch--result-columns '("name")
                clutch--result-column-defs '((:name "name" :type-category text))
                clutch--result-column-details
                (list (list :name "name" :type "VARCHAR(255)" :nullable t)))
    (insert (clutch--render-cell '("alice") 0 0 [8] nil))
    (goto-char 2)
    (let (seen)
      (cl-letf (((symbol-function 'message)
                 (lambda (fmt &rest args)
                   (setq seen (apply #'format fmt args)))))
        (clutch-result-column-info)
        (should (string-match-p "name" seen))
        (should (string-match-p "Type: VARCHAR(255)" seen))))))

(ert-deftest clutch-test-ensure-column-details-failure-is-memoized ()
  "Repeated sync detail loads should not reissue the same failing RPC."
  (let ((clutch--column-details-cache (make-hash-table :test 'equal))
        (clutch--column-details-status-cache (make-hash-table :test 'equal))
        (calls 0))
    (cl-letf (((symbol-function 'clutch--connection-key)
               (lambda (_conn) "dev-key"))
              ((symbol-function 'clutch-db-column-details)
               (lambda (_conn _table)
                 (cl-incf calls)
                 (signal 'clutch-db-error '("detail load failed")))))
      (should-not (clutch--ensure-column-details 'fake-conn "users"))
      (should-not (clutch--ensure-column-details 'fake-conn "users"))
      (should (= calls 1))
      (should (eq (plist-get (clutch--column-details-status 'fake-conn "users")
                             :state)
                  'failed)))))

(ert-deftest clutch-test-ensure-table-comment-does-not-cache-db-errors ()
  "Transient table-comment failures should not be memoized as missing comments."
  (let ((clutch--table-comment-cache (make-hash-table :test 'equal))
        (calls 0))
    (cl-letf (((symbol-function 'clutch--connection-key)
               (lambda (_conn) "dev-key"))
              ((symbol-function 'clutch-db-table-comment)
               (lambda (_conn _table)
                 (cl-incf calls)
                 (if (= calls 1)
                     (signal 'clutch-db-error '("comment boom"))
                   "Orders table"))))
      (should-not (clutch--ensure-table-comment 'fake-conn "orders"))
      (should (equal (clutch--ensure-table-comment 'fake-conn "orders")
                     "Orders table"))
      (should (= calls 2)))))

(ert-deftest clutch-test-ensure-help-doc-does-not-cache-transient-query-errors ()
  "Transient HELP lookup failures should not be memoized as missing docs."
  (let ((clutch--help-doc-cache (make-hash-table :test 'equal))
        (calls 0))
    (cl-letf (((symbol-function 'clutch--connection-key)
               (lambda (_conn) "dev-key"))
              ((symbol-function 'clutch-db-symbol-help)
               (lambda (_conn _symbol)
                 (cl-incf calls)
                 (if (= calls 1)
                     (signal 'clutch-db-error '("help boom"))
                   '(:sig "ABS(X)" :desc "Returns absolute value.")))))
      (should-not (clutch--ensure-help-doc 'fake-conn "abs"))
      (should (string-match-p "ABS(X)"
                              (clutch--ensure-help-doc 'fake-conn "abs")))
      (should (= calls 2)))))

(ert-deftest clutch-test-ensure-columns-failure-is-memoized ()
  "Repeated sync column-name loads should not reissue the same failing RPC."
  (let ((clutch--columns-status-cache (make-hash-table :test 'equal))
        (schema (make-hash-table :test 'equal))
        (calls 0))
    (puthash "users" nil schema)
    (cl-letf (((symbol-function 'clutch--connection-key)
               (lambda (_conn) "dev-key"))
              ((symbol-function 'clutch-db-list-columns)
               (lambda (_conn _table)
                 (cl-incf calls)
                 (signal 'clutch-db-error '("column load failed")))))
      (should-not (clutch--ensure-columns 'fake-conn schema "users"))
      (should-not (clutch--ensure-columns 'fake-conn schema "users"))
      (should (= calls 1))
      (should (eq (plist-get (clutch--columns-status 'fake-conn "users")
                             :state)
                  'failed)))))

(ert-deftest clutch-test-ensure-columns-async-falls-back-to-sync-when-unavailable ()
  "Async column preheat should fall back to sync loading when unavailable."
  (let ((clutch--columns-status-cache (make-hash-table :test 'equal))
        (schema (make-hash-table :test 'equal))
        calls)
    (puthash "users" nil schema)
    (cl-letf (((symbol-function 'clutch--connection-key)
               (lambda (_conn) "dev-key"))
              ((symbol-function 'clutch-db-list-columns-async)
               (lambda (&rest _args) nil))
              ((symbol-function 'clutch-db-list-columns)
               (lambda (_conn _table)
                 (setq calls (1+ (or calls 0)))
                 '("id" "name")))
              ((symbol-function 'clutch--refresh-schema-status-ui) #'ignore))
      (should (clutch--ensure-columns-async 'fake-conn schema "users"))
      (should (= calls 1))
      (should (equal (gethash "users" schema) '("id" "name")))
      (should-not (plist-get (clutch--columns-status 'fake-conn "users")
                             :state)))))

(ert-deftest clutch-test-column-details-async-ignores-stale-callbacks ()
  "Old async detail callbacks should not overwrite newer table state."
  (let ((clutch--column-details-cache (make-hash-table :test 'equal))
        (clutch--column-details-status-cache (make-hash-table :test 'equal))
        (clutch--column-details-queue-cache (make-hash-table :test 'equal))
        (clutch--column-details-active-cache (make-hash-table :test 'equal))
        (clutch--metadata-ticket-counter 0)
        callback)
    (cl-letf (((symbol-function 'clutch--connection-key)
               (lambda (_conn) "dev-key"))
              ((symbol-function 'clutch--connection-alive-p)
               (lambda (_conn) t))
              ((symbol-function 'clutch-db-column-details-async)
               (lambda (_conn _table cb &optional _errback)
                 (setq callback cb)
                 t))
              ((symbol-function 'clutch--refresh-schema-status-ui) #'ignore))
      (clutch--ensure-column-details-async 'fake-conn "users")
      (clutch--set-column-details-status
       'fake-conn "users" 'loading nil (clutch--begin-metadata-ticket))
      (funcall callback (list (list :name "stale_col" :type "int")))
      (should-not (clutch--cached-column-details 'fake-conn "users")))))

(ert-deftest clutch-test-column-details-async-ignores-dead-connection-callbacks ()
  "Async detail callbacks should be ignored after the connection dies."
  (let ((clutch--column-details-cache (make-hash-table :test 'equal))
        (clutch--column-details-status-cache (make-hash-table :test 'equal))
        (clutch--column-details-queue-cache (make-hash-table :test 'equal))
        (clutch--column-details-active-cache (make-hash-table :test 'equal))
        (clutch--metadata-ticket-counter 0)
        (alive t)
        callback)
    (cl-letf (((symbol-function 'clutch--connection-key)
               (lambda (_conn) "dev-key"))
              ((symbol-function 'clutch--connection-alive-p)
               (lambda (_conn) alive))
              ((symbol-function 'clutch-db-column-details-async)
               (lambda (_conn _table cb &optional _errback)
                 (setq callback cb)
                 t))
              ((symbol-function 'clutch--refresh-schema-status-ui) #'ignore))
      (clutch--ensure-column-details-async 'fake-conn "users")
      (setq alive nil)
      (funcall callback (list (list :name "dead_col" :type "int")))
      (should-not (clutch--cached-column-details 'fake-conn "users")))))

(ert-deftest clutch-test-column-details-async-falls-back-to-sync-when-unavailable ()
  "Async column-detail preheat should fall back to sync loading when unavailable."
  (let ((clutch--column-details-cache (make-hash-table :test 'equal))
        (clutch--column-details-status-cache (make-hash-table :test 'equal))
        (clutch--column-details-queue-cache (make-hash-table :test 'equal))
        (clutch--column-details-active-cache (make-hash-table :test 'equal))
        (clutch--metadata-ticket-counter 0)
        refreshed
        calls)
    (cl-letf (((symbol-function 'clutch--connection-key)
               (lambda (_conn) "dev-key"))
              ((symbol-function 'clutch--connection-alive-p)
               (lambda (_conn) t))
              ((symbol-function 'clutch-db-column-details-async)
               (lambda (&rest _args) nil))
              ((symbol-function 'clutch-db-column-details)
               (lambda (_conn _table)
                 (setq calls (1+ (or calls 0)))
                 (list (list :name "id" :type "int"))))
              ((symbol-function 'clutch--refresh-result-metadata-buffers)
               (lambda (_conn table)
                 (setq refreshed table)))
              ((symbol-function 'clutch--refresh-schema-status-ui) #'ignore))
      (should (clutch--ensure-column-details-async 'fake-conn "users"))
      (should (= calls 1))
      (should (equal refreshed "users"))
      (should (equal (clutch--cached-column-details 'fake-conn "users")
                     '((:name "id" :type "int"))))
      (should-not (clutch--column-details-active 'fake-conn)))))

(ert-deftest clutch-test-ensure-table-comment-async-falls-back-to-sync-when-unavailable ()
  "Async table-comment preheat should fall back to sync loading when unavailable."
  (let ((clutch--table-comment-cache (make-hash-table :test 'equal))
        (clutch--table-comment-status-cache (make-hash-table :test 'equal))
        calls)
    (cl-letf (((symbol-function 'clutch--connection-key)
               (lambda (_conn) "dev-key"))
              ((symbol-function 'clutch-db-table-comment-async)
               (lambda (&rest _args) nil))
              ((symbol-function 'clutch-db-table-comment)
               (lambda (_conn _table)
                 (setq calls (1+ (or calls 0)))
                 "Users table"))
              ((symbol-function 'clutch--refresh-schema-status-ui) #'ignore))
      (should (clutch--ensure-table-comment-async 'fake-conn "users"))
      (should (= calls 1))
      (should (equal (clutch--cached-table-comment 'fake-conn "users")
                     "Users table"))
      (should-not (plist-get (clutch--table-comment-status 'fake-conn "users")
                             :state)))))

(ert-deftest clutch-test-refresh-result-metadata-buffers-updates-only-matching-results ()
  "Result metadata refresh should only touch matching live result buffers."
  (let ((buf-a (generate-new-buffer " *clutch-result-a*"))
        (buf-b (generate-new-buffer " *clutch-result-b*"))
        (details '((:name "id" :type "int"))))
    (unwind-protect
        (cl-letf (((symbol-function 'clutch--connection-key)
                   (lambda (conn)
                     (pcase conn
                       ('conn-a "conn-a")
                       ('conn-b "conn-b")
                       (_ "other"))))
                  ((symbol-function 'clutch--result-column-details)
                   (lambda (_conn _table _col-names)
                     details)))
          (with-current-buffer buf-a
            (clutch-result-mode)
            (setq-local clutch-connection 'conn-a)
            (setq-local clutch--result-columns '("id"))
            (setq-local clutch--result-source-table "users")
            (setq-local clutch--last-query "select * from users"))
          (with-current-buffer buf-b
            (clutch-result-mode)
            (setq-local clutch-connection 'conn-b)
            (setq-local clutch--result-columns '("id"))
            (setq-local clutch--result-source-table "orders")
            (setq-local clutch--last-query "select * from orders"))
          (clutch--refresh-result-metadata-buffers 'conn-a "users")
          (with-current-buffer buf-a
            (should (equal clutch--result-column-details details)))
          (with-current-buffer buf-b
            (should-not clutch--result-column-details)))
      (when (buffer-live-p buf-a)
        (kill-buffer buf-a))
      (when (buffer-live-p buf-b)
        (kill-buffer buf-b)))))

(ert-deftest clutch-test-column-info-string-with-details ()
  "Column info string formats type and nullable info."
  (with-temp-buffer
    (setq-local clutch--result-columns '("id" "name"))
    (setq-local clutch--result-column-details
                (list (list :name "id" :type "INT" :nullable nil)
                      (list :name "name" :type "VARCHAR(255)" :nullable t
                            :default "unnamed")))
    (let ((info0 (clutch--column-info-string 0))
          (info1 (clutch--column-info-string 1)))
      (should (string-match-p "Type: INT" info0))
      (should (string-match-p "Nullable: NO" info0))
      (should (string-match-p "Type: VARCHAR(255)" info1))
      (should (string-match-p "Nullable: YES" info1))
      (should (string-match-p "Default: unnamed" info1)))))

(ert-deftest clutch-test-column-info-string-is-propertized ()
  "Column info string should carry faces for minibuffer highlighting."
  (with-temp-buffer
    (setq-local clutch--result-columns '("id"))
    (setq-local clutch--result-column-details
                (list (list :name "id" :type "INT" :nullable nil
                            :default "42" :comment "Primary key")))
    (let* ((info (clutch--column-info-string 0))
           (one-line (clutch--column-info-message-string info))
           (name-pos (string-match-p "\\bid\\b" one-line))
           (type-pos (string-match-p "INT" one-line))
           (sep-pos (string-match-p "  •  " one-line)))
      (should name-pos)
      (should type-pos)
      (should sep-pos)
      (should (eq (get-text-property name-pos 'face one-line)
                  'clutch-field-name-face))
      (should (eq (get-text-property type-pos 'face one-line)
                  'font-lock-type-face))
      (should (eq (get-text-property sep-pos 'face one-line)
                  'font-lock-comment-face)))))

(ert-deftest clutch-test-message-formatting-helpers-apply-faces ()
  "Echo-area formatting helpers should produce propertized strings."
  (let ((count (clutch--message-count 42))
        (ident (clutch--message-ident "users"))
        (keyword (clutch--message-keyword "CSV"))
        (literal (clutch--message-literal "\"needle\""))
        (path (clutch--message-path "/tmp/staged.sql")))
    (should (equal count "42"))
    (should (eq (get-text-property 0 'face count) 'font-lock-constant-face))
    (should (eq (get-text-property 0 'face ident) 'clutch-field-name-face))
    (should (eq (get-text-property 0 'face keyword) 'font-lock-keyword-face))
    (should (eq (get-text-property 0 'face literal) 'font-lock-string-face))
    (should (eq (get-text-property 0 'face path) 'font-lock-doc-face))))

(ert-deftest clutch-test-column-info-string-nil-when-no-details ()
  "Column info string returns nil when details are unavailable."
  (with-temp-buffer
    (setq-local clutch--result-columns '("id"))
    (setq-local clutch--result-column-details nil)
    (should-not (clutch--column-info-string 0))))

(ert-deftest clutch-test-result-column-details-maps-by-name ()
  "Detail resolution matches result columns to cached details by name."
  (cl-letf (((symbol-function 'clutch--cached-column-details)
             (lambda (_conn _table)
               (list (list :name "ID" :type "INT" :nullable nil)
                     (list :name "NAME" :type "VARCHAR" :nullable t)))))
    (let ((result (clutch--result-column-details
                   'dummy-conn "users" '("id" "name"))))
      (should (= (length result) 2))
      (should (equal (plist-get (nth 0 result) :type) "INT"))
      (should (equal (plist-get (nth 1 result) :type) "VARCHAR")))))

(ert-deftest clutch-test-result-column-details-nil-for-no-table ()
  "Detail resolution returns nil when no source table is detected."
  (should-not (clutch--result-column-details
               'dummy nil '("col1"))))

;;;; Edit — cell editing

(defun clutch-test--make-edit-cell-result-buffer (columns column-defs rows
                                                          &rest locals)
  "Return a result buffer prepared for edit-cell tests.
COLUMNS, COLUMN-DEFS, and ROWS initialize the result grid.  LOCALS is a
plist of additional buffer-local variables to set."
  (let ((buf (generate-new-buffer "*clutch-result*")))
    (with-current-buffer buf
      (setq-local clutch-connection 'fake-conn
                  clutch--result-columns columns
                  clutch--result-column-defs column-defs
                  clutch--result-rows rows)
      (while locals
        (set (make-local-variable (pop locals))
             (pop locals))))
    buf))

(defun clutch-test--open-edit-cell (result-buf cell table &optional details)
  "Open an edit buffer from RESULT-BUF for CELL on TABLE.
DETAILS, when non-nil, is returned by `clutch--ensure-column-details'."
  (with-current-buffer result-buf
    (setq-local clutch--result-source-table table))
  (cl-letf (((symbol-function 'clutch--cell-at-point)
             (lambda () cell))
            ((symbol-function 'clutch--ensure-column-details)
             (lambda (_conn _table &optional _strict)
               details))
            ((symbol-function 'pop-to-buffer)
             (lambda (buf &rest _args) buf)))
    (with-current-buffer result-buf
      (clutch-result-edit-cell))))

(ert-deftest clutch-test-edit-pending-insert-reopens-prefilled-insert-buffer ()
  "Editing a ghost insert row should reopen the staged insert with its values."
  (let ((result-buf (generate-new-buffer "*clutch-result*")))
    (unwind-protect
        (with-current-buffer result-buf
          (setq-local clutch--result-columns '("id" "severity" "owner")
                      clutch--result-source-table "shipping_incidents"
                      clutch--result-rows '((1 "low" "alice"))
                      clutch--pending-inserts '((("severity" . "high")
                                                 ("owner" . "bob"))))
          (cl-letf (((symbol-function 'clutch--cell-at-point)
                     (lambda () (list 1 1 "high")))
                    ((symbol-function 'pop-to-buffer)
                     (lambda (buf &rest _args) buf)))
            (let ((buf (clutch-result-edit-cell)))
              (with-current-buffer buf
                (should (equal clutch-result-insert--pending-index 0))
                (should (equal clutch-result-insert--table "shipping_incidents"))
                (should (string-match-p "^severity[ ]*: high$" (buffer-string)))
                (should (string-match-p "^owner[ ]*: bob$" (buffer-string)))))))
      (kill-buffer result-buf))))

(ert-deftest clutch-test-edit-cell-shows-metadata-and-completion-hints ()
  "Edit buffer should expose enum metadata and completion affordances."
  (let ((result-buf (clutch-test--make-edit-cell-result-buffer
                     '("severity")
                     '((:name "severity" :type-category text))
                     '(("low"))
                     'clutch--row-identity
                     (clutch-test--primary-row-identity
                      "shipping_incidents" '("severity") '(0)))))
    (unwind-protect
        (let ((buf (clutch-test--open-edit-cell
                    result-buf
                    '(0 0 "low")
                    "shipping_incidents"
                    (list (list :name "severity"
                                :type "enum('low','medium','high')")))))
          (with-current-buffer buf
            (should (string-match-p "\\[enum\\]" (format "%s" header-line-format)))
            (should (string-match-p "M-TAB: complete" (format "%s" header-line-format)))
            (pcase-let ((`(,beg ,end ,candidates . ,_)
                         (clutch-result-edit-completion-at-point)))
              (should (= beg (point-min)))
              (should (= end (point-max)))
              (should (equal candidates '("low" "medium" "high"))))))
      (kill-buffer result-buf))))

(ert-deftest clutch-test-edit-cell-errors-clearly-without-row-identity ()
  "Edit entry should fail early when the result is not updateable."
  (let ((result-buf (clutch-test--make-edit-cell-result-buffer
                     '("id" "name")
                     '((:name "id" :type-category numeric)
                       (:name "name" :type-category text))
                     '((1 "alice"))
                     'clutch--result-source-table "users"
                     'clutch--last-query "SELECT * FROM users")))
    (unwind-protect
        (cl-letf (((symbol-function 'clutch--cell-at-point)
                   (lambda () '(0 1 "alice"))))
          (with-current-buffer result-buf
            (let ((err (should-error (clutch-result-edit-cell)
                                     :type 'user-error)))
              (should (string-match-p
                       "Cannot edit cell: no primary, unique, or row locator identity available for table users"
                       (error-message-string err))))
            (should-not (get-buffer "*clutch-edit: [0].name*"))))
      (kill-buffer result-buf))))

(ert-deftest clutch-test-edit-cell-shows-temporal-now-hint ()
  "Temporal edit buffers should advertise the shared now shortcut."
  (let ((result-buf (clutch-test--make-edit-cell-result-buffer
                     '("opened_at")
                     '((:name "opened_at" :type-category datetime))
                     '(("2026-03-10 10:00:00"))
                     'clutch--row-identity
                     (clutch-test--primary-row-identity
                      "shipping_incidents" '("opened_at") '(0)))))
    (unwind-protect
        (let ((buf (clutch-test--open-edit-cell
                    result-buf
                    '(0 0 "2026-03-10 10:00:00")
                    "shipping_incidents"
                    (list (list :name "opened_at" :type "datetime")))))
          (with-current-buffer buf
            (should (string-match-p "\\[datetime\\]" (format "%s" header-line-format)))
            (should (string-match-p (regexp-quote "C-c .: now")
                                    (format "%s" header-line-format)))))
      (kill-buffer result-buf))))

(ert-deftest clutch-test-edit-cell-opens-json-sub-editor-directly ()
  "JSON cells should jump straight into the JSON sub-editor."
  (let ((result-buf (clutch-test--make-edit-cell-result-buffer
                     '("payload")
                     '((:name "payload" :type-category json))
                     '(("{\"a\":1}"))
                     'clutch--row-identity
                     (clutch-test--primary-row-identity
                      "shipping_incidents" '("payload") '(0)))))
    (unwind-protect
        (let ((buf (clutch-test--open-edit-cell
                    result-buf
                    '(0 0 "{\"a\":1}")
                    "shipping_incidents"
                    (list (list :name "payload" :type "json")))))
          (should (string-match-p "\\*clutch-edit-json: payload\\*" (buffer-name buf)))
          (with-current-buffer buf
            (should (equal clutch-result-edit-json--field-name "payload"))
            (should (string-match-p "JSON field payload"
                                    (format "%s" header-line-format)))
            (should (equal (buffer-substring-no-properties (point-min) (point-max))
                           "{\n  \"a\": 1\n}"))))
      (kill-buffer result-buf))))

(ert-deftest clutch-test-edit-cell-json-object-opens-sub-editor-with-json-text ()
  "Parsed JSON objects should reach the JSON sub-editor as JSON text."
  (skip-unless (fboundp 'json-serialize))
  (let* ((payload (make-hash-table :test 'equal))
         (result-buf (clutch-test--make-edit-cell-result-buffer
                      '("payload")
                      '((:name "payload" :type-category json))
                      (list (list payload))
                      'clutch--row-identity
                      (clutch-test--primary-row-identity
                       "shipping_incidents" '("payload") '(0)))))
    (puthash "test" t payload)
    (puthash "data" (vector 1 2) payload)
    (unwind-protect
        (let ((buf (clutch-test--open-edit-cell
                    result-buf
                    (list 0 0 payload)
                    "shipping_incidents"
                    (list (list :name "payload" :type "json")))))
          (with-current-buffer buf
            (should-not (string-match-p "#s(hash-table"
                                        (buffer-substring-no-properties
                                         (point-min) (point-max))))
            (should (string-match-p "\"test\": true"
                                    (buffer-substring-no-properties
                                     (point-min) (point-max))))
            (should (string-match-p "\"data\": \\["
                                    (buffer-substring-no-properties
                                     (point-min) (point-max))))))
      (kill-buffer result-buf))))

(ert-deftest clutch-test-edit-cell-json-string-opens-sub-editor-with-json-text ()
  "Parsed JSON string scalars should stay valid JSON in the sub-editor."
  (skip-unless (fboundp 'json-serialize))
  (let* ((payload "hello")
         (result-buf (clutch-test--make-edit-cell-result-buffer
                      '("payload")
                      '((:name "payload" :type-category json))
                      (list (list payload))
                      'clutch--row-identity
                      (clutch-test--primary-row-identity
                       "shipping_incidents" '("payload") '(0)))))
    (unwind-protect
        (let ((buf (clutch-test--open-edit-cell
                    result-buf
                    (list 0 0 payload)
                    "shipping_incidents"
                    (list (list :name "payload" :type "json")))))
          (with-current-buffer buf
            (should (equal (buffer-substring-no-properties (point-min) (point-max))
                           "\"hello\""))))
      (kill-buffer result-buf))))

(ert-deftest clutch-test-edit-set-current-time-replaces-existing-value ()
  "The edit-buffer current-time helper should replace the current value with now."
  (with-temp-buffer
    (insert "2020-01-01 00:00:00")
    (clutch--result-edit-mode 1)
    (setq-local clutch-result-edit--column-name "opened_at"
                clutch-result-edit--column-def '(:name "opened_at" :type-category datetime)
                clutch-result-edit--column-detail '(:name "opened_at" :type "datetime"))
    (cl-letf (((symbol-function 'current-time)
               (lambda () (encode-time 30 45 13 12 3 2026))))
      (clutch-result-edit-set-current-time)
      (should (equal (buffer-string) "2026-03-12 13:45:30")))))

;;;; Edit — insert buffer

(ert-deftest clutch-test-insert-buffer-tab-navigation ()
  "Insert buffer TAB navigation should jump between field value positions."
  (with-temp-buffer
    (insert "id: \nname: alice\ncreated_at: \n")
    (clutch-result-insert-mode 1)
    (goto-char (point-min))
    (goto-char (clutch-result-insert--current-field-value-position))
    (should (= (current-column) (length "id: ")))
    (clutch-result-insert-next-field)
    (should (equal (buffer-substring-no-properties
                    (line-beginning-position) (line-end-position))
                   "name: alice"))
    (should (= (point) (line-end-position)))
    (clutch-result-insert-next-field)
    (should (equal (buffer-substring-no-properties
                    (line-beginning-position) (line-end-position))
                   "created_at: "))
    (should (= (current-column) (length "created_at: ")))
    (clutch-result-insert-prev-field)
    (should (equal (buffer-substring-no-properties
                    (line-beginning-position) (line-end-position))
                   "name: alice"))
    (should (= (point) (line-end-position)))))

(ert-deftest clutch-test-insert-mode-annotates-existing-buffer-lines ()
  "Insert mode should annotate hand-written form text with field properties."
  (with-temp-buffer
    (insert "severity: high\nowner: bob\n")
    (clutch-result-insert-mode 1)
    (goto-char (point-min))
    (should (equal (get-text-property (point) 'clutch-insert-field-name)
                   "severity"))
    (search-forward "bob")
    (backward-char 1)
    (should (equal (get-text-property (point) 'clutch-insert-field-name)
                   "owner"))
    (should (equal (clutch-result-insert--current-field-name) "owner"))))

(ert-deftest clutch-test-insert-return-key-navigates-like-tab ()
  "RET should advance to the next insert field without replacing TAB."
  (with-temp-buffer
    (insert "id: \nname: alice\ncreated_at: \n")
    (clutch-result-insert-mode 1)
    (goto-char (point-min))
    (goto-char (clutch-result-insert--current-field-value-start))
    (call-interactively (key-binding (kbd "RET")))
    (should (equal (clutch-result-insert--current-field-name) "name"))
    (call-interactively (key-binding (kbd "TAB")))
    (should (equal (clutch-result-insert--current-field-name) "created_at"))))

(ert-deftest clutch-test-insert-buffer-renders-tight-tags-and-highlights-current-line ()
  "Insert buffer should keep tags tight to the colon and highlight the active field line."
  (let ((result-buf (generate-new-buffer "*clutch-insert-result*")))
    (unwind-protect
        (progn
          (with-current-buffer result-buf
            (setq-local clutch-connection 'fake-conn
                        clutch--result-columns '("id" "severity" "is_ship_blocked")
                        clutch--result-column-defs '((:name "id" :type-category numeric)
                                                     (:name "severity" :type-category text)
                                                     (:name "is_ship_blocked" :type-category numeric))))
          (with-temp-buffer
            (clutch-result-insert-mode 1)
            (setq-local clutch-result-insert--result-buffer result-buf
                        clutch-result-insert--table "shipping_incidents"
                        clutch-result-insert--show-all-fields t)
            (cl-letf (((symbol-function 'clutch--ensure-column-details)
                       (lambda (_conn _table)
                         (list (list :name "id" :type "int" :generated t :nullable nil)
                               (list :name "severity" :type "enum('low','medium')" :nullable nil)
                               (list :name "is_ship_blocked" :type "tinyint(1)" :default "0" :nullable nil)))))
              (clutch-result-insert--populate-buffer
               "shipping_incidents" '("id" "severity" "is_ship_blocked"))
              (let (prefixes)
                (goto-char (point-min))
                (while (not (eobp))
                  (let* ((field (clutch-result-insert--current-field-or-error))
                         (bounds (clutch-result-insert--field-value-bounds field)))
                    (push (buffer-substring-no-properties (line-beginning-position)
                                                          (car bounds))
                          prefixes))
                  (forward-line 1))
                (setq prefixes (nreverse prefixes))
                (should (string-match-p "^id[ ]+\\[generated\\]: $"
                                        (nth 0 prefixes)))
                (should (string-match-p "^severity[ ]+\\[enum required\\]: $"
                                        (nth 1 prefixes)))
                (should (string-match-p "^is_ship_blocked \\[default=0 bool\\]: $"
                                        (nth 2 prefixes)))))
              (goto-char (point-min))
              (clutch-result-insert-next-field)
              (let ((ov clutch-result-insert--active-field-overlay))
                (should (overlayp ov))
                (should (eq (overlay-get ov 'face) 'clutch-insert-active-field-face))
                (should (string-match-p "^severity" (buffer-substring-no-properties
                                                     (overlay-start ov)
                                                     (overlay-end ov))))
              (let ((prefix-ov clutch-result-insert--active-prefix-overlay))
                (should (overlayp prefix-ov))
                (should (eq (overlay-get prefix-ov 'face)
                            'clutch-insert-active-field-name-face))
                (should (string-match-p "^severity[ ]+\\[enum required\\]$"
                                        (buffer-substring-no-properties
                                         (overlay-start prefix-ov)
                                         (overlay-end prefix-ov))))))))
      (kill-buffer result-buf))))

(ert-deftest clutch-test-pending-insert-renders-generated-and-default-placeholders ()
  "Staged insert rows should show generated/default placeholders when known."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn
                clutch--result-columns '("id" "name" "created_at" "notes")
                clutch--result-source-table "users"
                clutch--result-column-defs '((:name "id" :type-category numeric)
                                             (:name "name" :type-category text)
                                             (:name "created_at" :type-category datetime)
                                             (:name "notes" :type-category text))
                clutch--pending-inserts '((("name" . "alice"))))
    (let ((row-positions (make-vector 1 nil))
          render-state)
      (cl-letf (((symbol-function 'clutch--ensure-column-details)
                 (lambda (_conn _table)
                   (list (list :name "id" :generated t)
                         (list :name "name")
                         (list :name "created_at" :default "CURRENT_TIMESTAMP")
                         (list :name "notes")))))
        (setq render-state (clutch--build-render-state))
        (clutch--insert-pending-insert-rows '(0 1 2 3) [12 12 12 12] 3 0 row-positions
                                            render-state)
        (let ((rendered (buffer-string)))
          (should (string-match-p "<generated>" rendered))
          (should (string-match-p "<default>" rendered))
          (should (string-match-p "alice" rendered)))))))

(ert-deftest clutch-test-pending-insert-uses-insert-markers ()
  "Staged insert rows should use insert semantics in the left prefix."
  (with-temp-buffer
    (setq-local clutch--result-columns '("id" "name")
                clutch--result-column-defs '((:name "id" :type-category numeric)
                                             (:name "name" :type-category text))
                clutch--pending-inserts '((("id" . "99") ("name" . "new"))))
    (let ((row-positions (make-vector 1 nil)))
      (clutch--insert-pending-insert-rows '(0 1) [8 8] 3 0 row-positions
                                          (clutch--build-render-state))
      (should (string-prefix-p "│I I1 " (buffer-string))))))

(ert-deftest clutch-test-insert-fill-current-time-respects-column-type ()
  "The insert buffer time-filling helper should use result column metadata."
  (let ((result-buf (generate-new-buffer "*clutch-insert-result*")))
    (unwind-protect
        (progn
          (with-current-buffer result-buf
            (setq-local clutch--result-columns '("due_on" "created_at" "name")
                        clutch--result-column-defs '((:name "due_on" :type-category date)
                                                     (:name "created_at" :type-category datetime)
                                                     (:name "name" :type-category text))))
          (with-temp-buffer
            (insert "due_on: 2024-01-01\ncreated_at: 2024-01-01 00:00:00\nname: alice\n")
            (clutch-result-insert-mode 1)
            (setq-local clutch-result-insert--result-buffer result-buf)
            (cl-letf (((symbol-function 'current-time)
                       (lambda () (encode-time 30 45 13 12 3 2026))))
              (goto-char (point-min))
              (clutch-result-insert-fill-current-time)
              (should (equal (buffer-substring-no-properties
                              (line-beginning-position) (line-end-position))
                             "due_on: 2026-03-12"))
              (forward-line 1)
              (clutch-result-insert-fill-current-time)
              (should (equal (buffer-substring-no-properties
                              (line-beginning-position) (line-end-position))
                             "created_at: 2026-03-12 13:45:30"))
              (forward-line 1)
              (should-error (clutch-result-insert-fill-current-time)
                            :type 'user-error))))
      (kill-buffer result-buf))))

(ert-deftest clutch-test-insert-buffer-labels-show_field_metadata ()
  "Insert buffer labels should show field metadata without changing parsed names."
  (let ((result-buf (generate-new-buffer "*clutch-insert-result*")))
    (unwind-protect
        (progn
          (with-current-buffer result-buf
            (setq-local clutch-connection 'fake-conn
                        clutch--result-columns '("id" "severity" "postmortem" "is_ship_blocked" "opened_at")
                        clutch--result-column-defs '((:name "id" :type-category numeric)
                                                     (:name "severity" :type-category text)
                                                     (:name "postmortem" :type-category json)
                                                     (:name "is_ship_blocked" :type-category numeric)
                                                     (:name "opened_at" :type-category datetime))))
          (with-temp-buffer
            (clutch-result-insert-mode 1)
            (setq-local clutch-result-insert--result-buffer result-buf
                        clutch-result-insert--table "shipping_incidents"
                        clutch-result-insert--show-all-fields t)
            (cl-letf (((symbol-function 'clutch--ensure-column-details)
                       (lambda (_conn _table)
                         (list (list :name "id" :type "int" :generated t :nullable nil)
                               (list :name "severity" :type "enum('low','medium')" :nullable nil)
                               (list :name "postmortem" :type "json" :nullable t)
                               (list :name "is_ship_blocked" :type "tinyint(1)" :default "0" :nullable nil)
                               (list :name "opened_at" :type "datetime" :nullable nil)))))
              (clutch-result-insert--populate-buffer "shipping_incidents"
                                                    '("id" "severity" "postmortem" "is_ship_blocked" "opened_at"))
              (let ((rendered (buffer-string)))
                (should (string-match-p "^id[ ]+\\[generated\\]: $" rendered))
                (should (string-match-p "^severity[ ]+\\[enum required\\]: $" rendered))
                (should (string-match-p "^postmortem[ ]+\\[json\\]: $" rendered))
                (should (string-match-p "^is_ship_blocked \\[default=0 bool\\]: $" rendered))
                (should (string-match-p "^opened_at[ ]+\\[datetime required\\]: $" rendered)))
              (goto-char (point-min))
              (should (get-text-property (point) 'read-only))
              (should (eq (get-text-property (point) 'face)
                          'clutch-field-name-face))
              (search-forward "[generated]")
              (should (eq (get-text-property (1- (point)) 'face)
                          'clutch-insert-field-tag-face))
              (goto-char (point-min))
              (search-forward "severity")
              (goto-char (clutch-result-insert--current-field-value-position))
              (insert "low")
              (goto-char (point-min))
              (let ((fields (clutch-result-insert--parse-fields)))
                (should (equal fields '(("severity" . "low"))))))))
      (kill-buffer result-buf))))

(ert-deftest clutch-test-insert-populate-buffer-reuses-read-only-buffer ()
  "Repopulating an insert buffer should work when prefixes are already read-only."
  (let ((result-buf (generate-new-buffer "*clutch-insert-result*")))
    (unwind-protect
        (progn
          (with-current-buffer result-buf
            (setq-local clutch-connection 'fake-conn
                        clutch--result-columns '("severity" "owner")
                        clutch--result-column-defs '((:name "severity" :type-category text)
                                                     (:name "owner" :type-category text))))
          (with-temp-buffer
            (clutch-result-insert-mode 1)
            (setq-local clutch-result-insert--result-buffer result-buf
                        clutch-result-insert--table "shipping_incidents")
            (cl-letf (((symbol-function 'clutch--ensure-column-details)
                       (lambda (_conn _table)
                         (list (list :name "severity"
                                     :type "enum('low','medium','high')")
                               (list :name "owner" :type "varchar(64)")))))
              (clutch-result-insert--populate-buffer
               "shipping_incidents" '("severity" "owner"))
              (should (get-text-property (point-min) 'read-only))
              (clutch-result-insert--populate-buffer
               "shipping_incidents" '("severity" "owner")
               '(("severity" . "high")))
              (should (string-match-p "^severity \\[enum required\\]: high$"
                                      (buffer-string))))))
      (kill-buffer result-buf))))

(ert-deftest clutch-test-insert-sparse-layout-toggle-preserves-hidden-values ()
  "Sparse insert layout should hide defaulted fields without dropping their values."
  (let ((result-buf (generate-new-buffer "*clutch-insert-result*"))
        insert-buf)
    (unwind-protect
        (progn
          (with-current-buffer result-buf
            (setq-local clutch-connection 'fake-conn
                        clutch--result-columns '("id" "severity" "owner" "created_at")
                        clutch--result-column-defs '((:name "id" :type-category numeric)
                                                     (:name "severity" :type-category text)
                                                     (:name "owner" :type-category text)
                                                     (:name "created_at" :type-category datetime))))
          (cl-letf (((symbol-function 'clutch--ensure-column-details)
                     (lambda (_conn _table)
                       (list (list :name "id" :type "int" :generated t :nullable nil)
                             (list :name "severity" :type "enum('low','medium','high')" :nullable nil)
                             (list :name "owner" :type "varchar(64)" :default "system" :nullable t)
                             (list :name "created_at" :type "datetime"
                                   :default "CURRENT_TIMESTAMP" :nullable t))))
                    ((symbol-function 'pop-to-buffer)
                     (lambda (buf &rest _args)
                       (setq insert-buf buf)
                       buf)))
            (clutch-result-insert--open-buffer "shipping_incidents" result-buf))
          (with-current-buffer insert-buf
            (should (string-match-p "^severity[ ]+\\[enum required\\]: $" (buffer-string)))
            (should-not (string-match-p "^owner" (buffer-string)))
            (should-not (string-match-p "^created_at" (buffer-string)))
            (clutch-result-insert-toggle-field-layout)
            (goto-char (point-min))
            (re-search-forward "^owner.*: " nil t)
            (insert "bob")
            (clutch-result-insert-toggle-field-layout)
            (should (string-match-p "^severity[ ]+\\[enum required\\]: $"
                                    (buffer-string)))
            (clutch-result-insert-toggle-field-layout)
            (should (string-match-p "^owner[ ]+\\[default=system\\]: bob$"
                                    (buffer-string)))))
      (when (buffer-live-p insert-buf)
        (kill-buffer insert-buf))
      (kill-buffer result-buf))))

(ert-deftest clutch-test-clone-row-to-insert-prefills-effective-result-values ()
  "Cloning a result row should reuse visible row values and staged edits."
  (let ((result-buf (generate-new-buffer "*clutch-result*"))
        insert-buf)
    (unwind-protect
        (progn
          (with-current-buffer result-buf
            (clutch-result-mode)
            (setq-local clutch-connection 'fake-conn
                        clutch--last-query "SELECT * FROM shipping_incidents"
                        clutch--result-source-table "shipping_incidents"
                        clutch--result-columns '("id" "severity" "owner" "created_at")
                        clutch--result-column-defs '((:name "id" :type-category numeric)
                                                     (:name "severity" :type-category text)
                                                     (:name "owner" :type-category text)
                                                     (:name "created_at" :type-category datetime))
                        clutch--result-rows '((1 "low" "alice" "2026-03-01 10:00:00"))
                        clutch--filtered-rows nil
                        clutch--row-identity (clutch-test--primary-row-identity
                                              "shipping_incidents" '("id") '(0))
                        clutch--pending-edits '((([1] . 1) . "high"))))
          (cl-letf (((symbol-function 'clutch--row-idx-at-line)
                     (lambda () 0))
                    ((symbol-function 'clutch--ensure-column-details)
                     (lambda (_conn _table)
                       (list (list :name "id" :type "int" :generated t :nullable nil)
                             (list :name "severity" :type "enum('low','medium','high')" :nullable nil)
                             (list :name "owner" :type "varchar(64)" :nullable t)
                             (list :name "created_at" :type "datetime"
                                   :default "CURRENT_TIMESTAMP" :nullable t))))
                    ((symbol-function 'pop-to-buffer)
                     (lambda (buf &rest _args)
                       (setq insert-buf buf)
                       buf)))
            (with-current-buffer result-buf
              (clutch-clone-row-to-insert)))
          (with-current-buffer insert-buf
            (should-not (string-match-p "^id" (buffer-string)))
            (should (string-match-p "^severity[ ]+\\[enum required\\]: high$"
                                    (buffer-string)))
            (should (string-match-p "^owner[ ]*: alice$" (buffer-string)))
            (should (string-match-p "^created_at .*2026-03-01 10:00:00$"
                                    (buffer-string)))))
      (when (buffer-live-p insert-buf)
        (kill-buffer insert-buf))
      (kill-buffer result-buf))))

(ert-deftest clutch-test-clone-row-to-insert-works-from-record-buffer ()
  "Cloning from a record buffer should prefill from the current record row."
  (let ((result-buf (generate-new-buffer "*clutch-result*"))
        (record-buf (generate-new-buffer "*clutch-record*"))
        insert-buf)
    (unwind-protect
        (progn
          (with-current-buffer result-buf
            (setq-local clutch-connection 'fake-conn
                        clutch--last-query "SELECT * FROM shipping_incidents"
                        clutch--result-source-table "shipping_incidents"
                        clutch--result-columns '("id" "owner")
                        clutch--result-column-defs '((:name "id" :type-category numeric)
                                                     (:name "owner" :type-category text))
                        clutch--result-rows '((7 "carol"))))
          (with-current-buffer record-buf
            (clutch-record-mode)
            (setq-local clutch-record--result-buffer result-buf
                        clutch-record--row-idx 0))
          (cl-letf (((symbol-function 'clutch--ensure-column-details)
                     (lambda (_conn _table)
                       (list (list :name "id" :type "int" :generated t :nullable nil)
                             (list :name "owner" :type "varchar(64)" :nullable t))))
                    ((symbol-function 'pop-to-buffer)
                     (lambda (buf &rest _args)
                       (setq insert-buf buf)
                       buf)))
            (with-current-buffer record-buf
              (clutch-clone-row-to-insert)))
          (with-current-buffer insert-buf
            (should-not (string-match-p "^id" (buffer-string)))
            (should (string-match-p "^owner[ ]*: carol$" (buffer-string)))))
      (when (buffer-live-p insert-buf)
        (kill-buffer insert-buf))
      (when (buffer-live-p record-buf)
        (kill-buffer record-buf))
      (kill-buffer result-buf))))

(ert-deftest clutch-test-clone-row-to-insert-hides-primary-key-by-default ()
  "Clone-to-insert should not prefill or sparsely render primary-key fields."
  (let ((result-buf (generate-new-buffer "*clutch-result*"))
        insert-buf)
    (unwind-protect
        (progn
          (with-current-buffer result-buf
            (clutch-result-mode)
            (setq-local clutch-connection 'fake-conn
                        clutch--last-query "SELECT * FROM incident_codes"
                        clutch--result-source-table "incident_codes"
                        clutch--result-columns '("id" "label")
                        clutch--result-column-defs '((:name "id" :type-category numeric)
                                                     (:name "label" :type-category text))
                        clutch--result-rows '((42 "duplicate me"))
                        clutch--filtered-rows nil))
          (cl-letf (((symbol-function 'clutch--row-idx-at-line)
                     (lambda () 0))
                    ((symbol-function 'clutch--ensure-column-details)
                     (lambda (_conn _table)
                       (list (list :name "id" :type "int" :primary-key t :nullable nil)
                             (list :name "label" :type "varchar(64)" :nullable nil))))
                    ((symbol-function 'pop-to-buffer)
                     (lambda (buf &rest _args)
                       (setq insert-buf buf)
                       buf)))
            (with-current-buffer result-buf
              (clutch-clone-row-to-insert)))
          (with-current-buffer insert-buf
            (should-not (string-match-p "^id" (buffer-string)))
            (should (string-match-p "^label[ ]+\\[required\\]: duplicate me$"
                                    (buffer-string)))
            (clutch-result-insert-toggle-field-layout)
            (should (string-match-p "^id[ ]+\\[required\\]: $" (buffer-string)))))
      (when (buffer-live-p insert-buf)
        (kill-buffer insert-buf))
      (kill-buffer result-buf))))

(ert-deftest clutch-test-insert-import-delimited-parses-quoted-csv-row ()
  "Single-row CSV import should prefill the current insert form."
  (let ((result-buf (generate-new-buffer "*clutch-insert-result*"))
        insert-buf)
    (unwind-protect
        (progn
          (with-current-buffer result-buf
            (setq-local clutch-connection 'fake-conn
                        clutch--result-columns '("owner" "severity")
                        clutch--result-column-defs '((:name "owner" :type-category text)
                                                     (:name "severity" :type-category text))))
          (cl-letf (((symbol-function 'clutch--ensure-column-details)
                     (lambda (_conn _table)
                       (list (list :name "owner" :type "varchar(64)" :nullable t)
                             (list :name "severity" :type "enum('low','high')" :nullable nil))))
                    ((symbol-function 'pop-to-buffer)
                     (lambda (buf &rest _args)
                       (setq insert-buf buf)
                       buf)))
            (clutch-result-insert--open-buffer "shipping_incidents" result-buf))
          (with-current-buffer insert-buf
            (clutch-result-insert-import-delimited
             "owner,severity\n\"Bob, Jr.\",high\n")
            (should (string-match-p "^owner[ ]*: Bob, Jr\\.$" (buffer-string)))
            (should (string-match-p "^severity[ ]+\\[enum required\\]: high$"
                                    (buffer-string)))))
      (when (buffer-live-p insert-buf)
        (kill-buffer insert-buf))
      (kill-buffer result-buf))))

(ert-deftest clutch-test-insert-import-delimited-stages-multi-row-header-mapping ()
  "Multi-row delimited import should stage inserts by header names."
  (let ((result-buf (generate-new-buffer "*clutch-insert-result*"))
        insert-buf)
    (unwind-protect
        (progn
          (with-current-buffer result-buf
            (setq-local clutch-connection 'fake-conn
                        clutch--result-columns '("severity" "owner" "created_at")
                        clutch--result-column-defs '((:name "severity" :type-category text)
                                                     (:name "owner" :type-category text)
                                                     (:name "created_at" :type-category datetime))
                        clutch--pending-inserts nil))
          (cl-letf (((symbol-function 'clutch--ensure-column-details)
                     (lambda (_conn _table)
                       (list (list :name "severity" :type "enum('low','high')" :nullable nil)
                             (list :name "owner" :type "varchar(64)" :nullable t)
                             (list :name "created_at" :type "datetime"
                                   :default "CURRENT_TIMESTAMP" :nullable t))))
                    ((symbol-function 'clutch--refresh-display) #'ignore)
                    ((symbol-function 'pop-to-buffer)
                     (lambda (buf &rest _args)
                       (setq insert-buf buf)
                       buf)))
            (clutch-result-insert--open-buffer "shipping_incidents" result-buf))
          (with-current-buffer insert-buf
            (clutch-result-insert-import-delimited
             "owner\tseverity\nbob\thigh\nann\tlow\n")
            (should (equal (with-current-buffer result-buf
                             clutch--pending-inserts)
                           '((("owner" . "bob") ("severity" . "high"))
                              (("owner" . "ann") ("severity" . "low")))))
            (should (string-match-p "^severity[ ]+\\[enum required\\]: $"
                                    (buffer-string)))))
      (when (buffer-live-p insert-buf)
        (kill-buffer insert-buf))
      (kill-buffer result-buf))))

;;;; Edit — staged mutations (row identity)

(ert-deftest clutch-test-insert-commit-replaces-existing-pending-insert ()
  "Committing a re-edited insert should replace the staged entry in place."
  (let ((result-buf (generate-new-buffer "*clutch-result*")))
    (unwind-protect
        (with-current-buffer result-buf
          (setq-local clutch--pending-inserts '((("severity" . "low"))))
          (cl-letf (((symbol-function 'clutch--refresh-display) #'ignore)
                    ((symbol-function 'quit-window) #'ignore))
            (with-temp-buffer
              (insert "severity: high\n")
              (clutch-result-insert-mode 1)
              (setq-local clutch-result-insert--result-buffer result-buf
                          clutch-result-insert--pending-index 0)
              (clutch-result-insert-commit))
            (should (equal clutch--pending-inserts
                           '((("severity" . "high")))))))
      (kill-buffer result-buf))))

(ert-deftest clutch-test-build-render-state-creates-fast-lookups ()
  "Render-state tables should preserve staged edit/delete/mark semantics."
  (with-temp-buffer
    (setq-local clutch--pending-edits '((([1] . 1) . "edited")))
    (setq-local clutch--pending-deletes '([1]))
    (setq-local clutch--marked-rows '(0 2))
    (let* ((state (clutch--build-render-state))
           (edits (plist-get state :edits))
           (edit-rows (plist-get state :edit-rows))
           (marked (plist-get state :marked))
           (deletes (plist-get state :deletes)))
      (should (equal (gethash '([1] . 1) edits)
                     '(([1] . 1) . "edited")))
      (should (gethash [1] edit-rows))
      (should (gethash 0 marked))
      (should (gethash 2 marked))
      (should (gethash [1] deletes)))))

(ert-deftest clutch-test-apply-edit-errors-clearly-without-row-identity ()
  "Edit staging should explain why update/delete are disabled."
  (with-temp-buffer
    (setq-local clutch--last-query "SELECT * FROM users"
                clutch--result-source-table "users"
                clutch--result-rows '((1 "before"))
                clutch--filtered-rows nil)
    (let ((err (should-error (clutch-result--apply-edit 0 1 "after")
                             :type 'user-error)))
      (should (string-match-p
               "no primary, unique, or row locator identity available for table users"
               (error-message-string err))))))

(ert-deftest clutch-test-apply-edit-with-filter-noop-uses-visible-row ()
  "Edit staging should compare against the filtered display row."
  (with-temp-buffer
    (setq-local clutch--last-query "SELECT id, name FROM users"
                clutch--result-columns '("id" "name")
                clutch--result-source-table "users"
                clutch--result-rows '((1 "alpha") (2 "beta"))
                clutch--filtered-rows '((2 "beta"))
                clutch--row-identity (clutch-test--primary-row-identity
                                      "users" '("id") '(0))
                clutch--pending-edits nil)
    (cl-letf (((symbol-function 'clutch--replace-row-at-index) #'ignore)
              ((symbol-function 'clutch--refresh-footer-line) #'ignore)
              ((symbol-function 'message) #'ignore))
      (clutch-result--apply-edit 0 1 "beta")
      (should-not clutch--pending-edits))))

(ert-deftest clutch-test-delete-rows-errors-clearly-without-row-identity ()
  "Delete staging should explain why update/delete are disabled."
  (with-temp-buffer
    (setq-local clutch--last-query "SELECT * FROM users"
                clutch--result-source-table "users"
                clutch--result-rows '((1 "before"))
                clutch--filtered-rows nil)
    (cl-letf (((symbol-function 'clutch--selected-row-indices) (lambda () '(0)))
              ((symbol-function 'clutch--refresh-display) #'ignore))
      (let ((err (should-error (clutch-result-delete-rows)
                               :type 'user-error)))
        (should (string-match-p
                 "no primary, unique, or row locator identity available for table users"
                 (error-message-string err)))))))

(ert-deftest clutch-test-build-delete-stmt-variants ()
  "DELETE builder should handle primary-key variants correctly."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn)
    (cl-letf (((symbol-function 'clutch-db-escape-identifier)
               (lambda (_conn id) (format "\"%s\"" id))))
      (dolist (case '(("single"
                       "orders"
                       (42 "alice" "2024-01-15")
                       ("id" "customer" "created_at")
                       (0)
                       "DELETE FROM \"orders\" WHERE \"id\" = ?"
                       (42))
                      ("compound"
                       "order_items"
                       (7 99 3)
                       ("order_id" "item_id" "qty")
                       (0 1)
                       "DELETE FROM \"order_items\" WHERE \"order_id\" = ? AND \"item_id\" = ?"
                       (7 99))
                      ("null"
                       "events"
                       (nil "orphan")
                       ("parent_id" "name")
                       (0)
                       "DELETE FROM \"events\" WHERE \"parent_id\" IS NULL"
                       ())))
        (pcase-let ((`(,label ,table ,row ,columns ,pk-indices ,sql ,params) case))
          (ert-info ((format "delete case: %s" label))
            (let* ((row-identity
                    (list :kind 'primary-key
                          :name "PRIMARY"
                          :table table
                          :columns (mapcar (lambda (i) (nth i columns))
                                           pk-indices)
                          :indices pk-indices))
                   (identity-vec (clutch-db-row-identity-values
                                  row row-identity))
                   (stmt (clutch-result--build-delete-stmt-for-identity
                          table identity-vec row-identity)))
              (should (equal (car stmt) sql))
              (should (equal (cdr stmt) params))
              (should-not (member nil (cdr stmt))))))))))

(ert-deftest clutch-test-build-insert-sql-returns-template-and-params ()
  "INSERT builder should return a template plus parameter values."
  (with-temp-buffer
    (cl-letf (((symbol-function 'clutch-db-escape-identifier)
               (lambda (_conn id) (format "\"%s\"" id))))
      (let ((stmt (clutch-result-insert--build-sql
                   'fake-conn
                   "users"
                   '(("id" . "7")
                     ("name" . "alice")
                     ("note" . "NULL")))))
        (should (equal (car stmt)
                       "INSERT INTO \"users\" (\"id\", \"name\", \"note\") VALUES (?, ?, ?)"))
        (should (equal (cdr stmt) '("7" "alice" nil)))))))

(ert-deftest clutch-test-stage-delete-stores-row-identity-vec ()
  "Staging a delete stores an identity vector, not a ridx integer."
  (with-temp-buffer
    (setq-local clutch--result-columns '("id" "name"))
    (setq-local clutch--result-source-table "users")
    (setq-local clutch--result-rows (list (list 42 "alice")))
    (setq-local clutch--row-identity (clutch-test--primary-row-identity
                                      "users" '("id") '(0)))
    (setq-local clutch--filtered-rows nil)
    (setq-local clutch--pending-deletes nil)
    (cl-letf (((symbol-function 'clutch--selected-row-indices) (lambda () '(0)))
              ((symbol-function 'clutch--refresh-display) #'ignore))
      (clutch-result-delete-rows)
      (should (equal clutch--pending-deletes (list (vector 42)))))))

(ert-deftest clutch-test-commit-delete-uses-row-identity-vec ()
  "DELETE statement uses identity values from stored vector, not ridx."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn)
    (setq-local clutch--result-columns '("id" "name"))
    (setq-local clutch--result-source-table "users")
    (setq-local clutch--row-identity (clutch-test--primary-row-identity
                                      "users" '("id") '(0)))
    (setq-local clutch--pending-deletes (list (vector 42)))
    (cl-letf (((symbol-function 'clutch-db-escape-identifier)
               (lambda (_conn name) (format "`%s`" name))))
      (let ((stmts (clutch-result--build-pending-delete-statements)))
        (should (= (length stmts) 1))
        (should (equal (caar stmts) "DELETE FROM `users` WHERE `id` = ?"))
        (should (equal (cdar stmts) '(42)))))))

(ert-deftest clutch-test-stage-edit-stores-row-identity-vec ()
  "Staging an edit stores (identity-vec . cidx) key, not (ridx . cidx)."
  (with-temp-buffer
    (setq-local clutch--result-columns '("id" "name"))
    (setq-local clutch--result-source-table "users")
    (setq-local clutch--result-rows (list (list 7 "bob")))
    (setq-local clutch--row-identity (clutch-test--primary-row-identity
                                      "users" '("id") '(0)))
    (setq-local clutch--filtered-rows nil)
    (setq-local clutch--pending-edits nil)
    (cl-letf (((symbol-function 'clutch--refresh-display) #'ignore))
      (clutch-result--apply-edit 0 1 "carol")
      (should (= (length clutch--pending-edits) 1))
      (let ((key (caar clutch--pending-edits)))
        (should (vectorp (car key)))
        (should (equal (car key) (vector 7)))
        (should (= (cdr key) 1))))))

(ert-deftest clutch-test-commit-edit-generates-update-with-primary-key-identity-where ()
  "UPDATE statement uses primary-key identity values in WHERE clause."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn)
    (setq-local clutch--result-columns '("id" "name"))
    (setq-local clutch--result-source-table "users")
    (setq-local clutch--row-identity (clutch-test--primary-row-identity
                                      "users" '("id") '(0)))
    (setq-local clutch--pending-edits
                (list (cons (cons (vector 7) 1) "carol")))
    (cl-letf (((symbol-function 'clutch-db-escape-identifier)
               (lambda (_conn name) (format "`%s`" name))))
      (let ((stmts (clutch-result--build-update-statements)))
        (should (= (length stmts) 1))
        (should (equal (caar stmts)
                       "UPDATE `users` SET `name` = ? WHERE `id` = ?"))
        (should (equal (cdar stmts) '("carol" 7)))))))

(ert-deftest clutch-test-build-update-stmt-uses-row-locator-where ()
  "UPDATE builder should use backend row locator predicates when available."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn)
    (cl-letf (((symbol-function 'clutch-db-escape-identifier)
               (lambda (_conn name) (format "\"%s\"" name))))
      (let* ((row-identity (list :kind 'row-locator
                                 :table "users"
                                 :indices '(0)
                                 :hidden-aliases '("clutch__rid_0")
                                 :where-sql "ctid = ?::tid"))
             (stmt (clutch-result--build-update-stmt
                    "users" (vector "(0,1)") '((1 . "carol"))
                    '("clutch__rid_0" "name") row-identity)))
        (should (equal (car stmt)
                       "UPDATE \"users\" SET \"name\" = ? WHERE ctid = ?::tid"))
        (should (equal (cdr stmt) '("carol" "(0,1)")))))))

(ert-deftest clutch-test-discard-delete-removes-row-identity-entry ()
  "Discarding a delete should remove the matching staged delete."
  (with-temp-buffer
    (setq-local clutch--result-columns '("id" "name"))
    (setq-local clutch--result-rows (list (list 42 "alice")))
    (setq-local clutch--row-identity (clutch-test--primary-row-identity
                                      "users" '("id") '(0)))
    (setq-local clutch--filtered-rows nil)
    (setq-local clutch--pending-deletes (list (vector 42)))
    (setq-local clutch--pending-edits nil)
    (setq-local clutch--pending-inserts nil)
    (cl-letf (((symbol-function 'clutch--row-idx-at-line) (lambda () 0))
              ((symbol-function 'clutch--refresh-display) #'ignore))
      (clutch-result-discard-pending-at-point)
      (should (null clutch--pending-deletes)))))

(ert-deftest clutch-test-discard-insert-removes-entry ()
  "Discarding a ghost insert row should remove the staged insert."
  (with-temp-buffer
    (setq-local clutch--result-columns '("id" "name"))
    (setq-local clutch--result-rows (list (list 1 "x")))
    (setq-local clutch--pending-inserts (list '(("id" . "99") ("name" . "new"))))
    (setq-local clutch--pending-deletes nil)
    (setq-local clutch--pending-edits nil)
    (cl-letf (((symbol-function 'clutch--row-idx-at-line)
               (lambda () 1))  ; ridx=1 >= nrows=1 → insert slot 0
              ((symbol-function 'clutch--refresh-display) #'ignore))
      (clutch-result-discard-pending-at-point)
      (should (null clutch--pending-inserts)))))

(ert-deftest clutch-test-discard-edit-removes-cell-entry ()
  "Discarding an edited cell should remove the matching staged edit."
  (with-temp-buffer
    (setq-local clutch--result-columns '("id" "name")
                clutch--result-rows (list (list 42 "alice"))
                clutch--row-identity (clutch-test--primary-row-identity
                                      "users" '("id") '(0))
                clutch--filtered-rows nil
                clutch--pending-edits (list (cons (cons (vector 42) 1) "carol"))
                clutch--pending-deletes nil
                clutch--pending-inserts nil)
    (cl-letf (((symbol-function 'clutch--row-idx-at-line) (lambda () 0))
              ((symbol-function 'clutch--col-idx-at-point) (lambda () 1))
              ((symbol-function 'clutch--refresh-display) #'ignore))
      (clutch-result-discard-pending-at-point)
      (should (null clutch--pending-edits)))))

(ert-deftest clutch-test-check-pending-changes-blocks-when-deletes-pending ()
  "`clutch-result--check-pending-changes' should signal when discard is declined."
  (let ((buf (generate-new-buffer "*clutch-result*")))
    (unwind-protect
        (with-current-buffer buf
          (setq-local clutch--pending-deletes (list (vector 1)))
          (setq-local clutch--pending-edits nil)
          (setq-local clutch--pending-inserts nil)
          (cl-letf (((symbol-function 'get-buffer)
                     (lambda (_name) buf))
                    ((symbol-function 'yes-or-no-p) (lambda (_) nil)))
            (should-error (clutch-result--check-pending-changes)
                          :type 'user-error)))
      (kill-buffer buf))))

(ert-deftest clutch-test-commit-ordering-insert-update-delete ()
  "Commit executes INSERT before UPDATE before DELETE."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn)
    (setq-local clutch--result-columns '("id" "name"))
    (setq-local clutch--result-rows (list (list 1 "a") (list 2 "b")))
    (setq-local clutch--pending-inserts '((("id" . "3") ("name" . "c"))))
    (setq-local clutch--pending-edits
                (list (cons (cons (vector 1) 1) "a2")))
    (setq-local clutch--pending-deletes (list (vector 2)))
    (let (executed)
      (cl-letf (((symbol-function 'clutch-result--build-pending-insert-statements)
                 (lambda () '(("INSERT INTO users (id, name) VALUES (?, ?)" . ("3" "c")))))
                ((symbol-function 'clutch-result--build-update-statements)
                 (lambda () '(("UPDATE users SET name = ? WHERE id = ?" . ("a2" 1)))))
                ((symbol-function 'clutch-result--build-pending-delete-statements)
                 (lambda () '(("DELETE FROM users WHERE id = ?" . (2)))))
                ((symbol-function 'clutch-db-escape-literal)
                 (lambda (_conn value) (format "'%s'" value)))
                ((symbol-function 'yes-or-no-p) (lambda (_) t))
                ((symbol-function 'clutch--run-db-query)
                 (lambda (_conn sql &optional params)
                   (push (cons sql params) executed)))
                ((symbol-function 'clutch--execute) #'ignore))
        (clutch-result-commit)
        (should (= (length executed) 3))
        ;; executed is in reverse push order: last executed is at (nth 0 executed)
        (should (string-prefix-p "INSERT" (car (nth 2 executed))))
        (should (equal (cdr (nth 2 executed)) '("3" "c")))
        (should (string-prefix-p "UPDATE" (car (nth 1 executed))))
        (should (equal (cdr (nth 1 executed)) '("a2" 1)))
        (should (string-prefix-p "DELETE" (car (nth 0 executed))))
        (should (equal (cdr (nth 0 executed)) '(2)))))))

;;;; Edit — validation

(ert-deftest clutch-test-insert-local-validation-shows-inline-error ()
  "Editing an invalid value should mark the current insert field inline."
  (let ((result-buf (generate-new-buffer "*clutch-insert-result*")))
    (unwind-protect
        (progn
          (with-current-buffer result-buf
            (setq-local clutch-connection 'fake-conn
                        clutch--result-columns '("impact_score")
                        clutch--result-column-defs '((:name "impact_score" :type-category numeric))))
          (with-temp-buffer
            (insert "impact_score: \n")
            (clutch-result-insert-mode 1)
            (setq-local clutch-result-insert--result-buffer result-buf
                        clutch-result-insert--table "shipping_incidents")
            (cl-letf (((symbol-function 'clutch--ensure-column-details)
                       (lambda (_conn _table)
                         (list (list :name "impact_score" :type "decimal(5,1)")))))
              (goto-char (clutch-result-insert--current-field-value-start))
              (insert "x")
              (let* ((field (clutch-result-insert--field-state "impact_score"))
                     (after (overlay-get (plist-get field :error-overlay)
                                         'after-string)))
                (should (equal (plist-get field :error-message)
                               "Field impact_score expects a numeric value"))
                (should (overlayp (plist-get field :error-overlay)))
                (should (string-match-p "\\[invalid numeric\\]" after))
                (should-not (string-prefix-p "\n" after))))))
      (kill-buffer result-buf))))

(ert-deftest clutch-test-insert-local-validation-clears-inline-error ()
  "Fixing a field should clear its inline validation message."
  (let ((result-buf (generate-new-buffer "*clutch-insert-result*")))
    (unwind-protect
        (progn
          (with-current-buffer result-buf
            (setq-local clutch-connection 'fake-conn
                        clutch--result-columns '("impact_score")
                        clutch--result-column-defs '((:name "impact_score" :type-category numeric))))
          (with-temp-buffer
            (insert "impact_score: x\n")
            (clutch-result-insert-mode 1)
            (setq-local clutch-result-insert--result-buffer result-buf
                        clutch-result-insert--table "shipping_incidents")
            (cl-letf (((symbol-function 'clutch--ensure-column-details)
                       (lambda (_conn _table)
                         (list (list :name "impact_score" :type "decimal(5,1)")))))
              (let ((field (clutch-result-insert--field-state "impact_score")))
                (clutch-result-insert--validate-field-live field)
                (setq field (clutch-result-insert--field-state "impact_score"))
                (should (plist-get field :error-message))
                (goto-char (clutch-result-insert--current-field-value-start))
                (delete-region (point) (line-end-position))
                (insert "1.5")
                (setq field (clutch-result-insert--field-state "impact_score"))
                (should-not (plist-get field :error-message))
                (should-not (plist-get field :error-overlay))))))
      (kill-buffer result-buf))))

(ert-deftest clutch-test-insert-json-validation-is-scheduled-on-idle ()
  "JSON fields should defer local validation until the user goes idle."
  (let ((result-buf (generate-new-buffer "*clutch-insert-result*"))
        (scheduled nil))
    (unwind-protect
        (progn
          (with-current-buffer result-buf
            (setq-local clutch-connection 'fake-conn
                        clutch--result-columns '("postmortem")
                        clutch--result-column-defs '((:name "postmortem" :type-category json))))
          (with-temp-buffer
            (insert "postmortem: \n")
            (clutch-result-insert-mode 1)
            (setq-local clutch-result-insert--result-buffer result-buf
                        clutch-result-insert--table "shipping_incidents")
            (cl-letf (((symbol-function 'clutch--ensure-column-details)
                       (lambda (_conn _table)
                         (list (list :name "postmortem" :type "json"))))
                      ((symbol-function 'run-with-idle-timer)
                       (lambda (secs _repeat fn &rest args)
                         (setq scheduled (list secs fn args))
                         'fake-timer)))
              (goto-char (clutch-result-insert--current-field-value-start))
              (insert "{")
              (should scheduled)
              (should (= (car scheduled) clutch-insert-validation-idle-delay))
              (should (eq (cadr scheduled)
                          #'clutch-result-insert--run-idle-validation)))))
      (kill-buffer result-buf))))

(ert-deftest clutch-test-edit-live-validation-shows-short-token ()
  "Edit buffers should surface a compact live-validation token."
  (with-temp-buffer
    (insert "xx")
    (clutch--result-edit-mode 1)
    (setq-local clutch-result-edit--row-idx 0
                clutch-result-edit--column-name "impact_score"
                clutch-result-edit--column-def '(:name "impact_score" :type-category numeric)
                clutch-result-edit--column-detail '(:name "impact_score" :type "decimal(5,1)"))
    (clutch-result-edit--refresh-header-line)
    (clutch-result-edit--validate-live)
    (should (equal clutch-result-edit--error-message
                   "Field impact_score expects a numeric value"))
    (should (string-match-p "\\[invalid numeric\\]"
                            (format "%s" header-line-format)))))

(ert-deftest clutch-test-edit-live-validation-clears-when-fixed ()
  "Fixing an invalid edit value should clear the live-validation token."
  (with-temp-buffer
    (insert "xx")
    (clutch--result-edit-mode 1)
    (setq-local clutch-result-edit--row-idx 0
                clutch-result-edit--column-name "impact_score"
                clutch-result-edit--column-def '(:name "impact_score" :type-category numeric)
                clutch-result-edit--column-detail '(:name "impact_score" :type "decimal(5,1)"))
    (clutch-result-edit--refresh-header-line)
    (clutch-result-edit--validate-live)
    (erase-buffer)
    (insert "1.5")
    (clutch-result-edit--validate-live)
    (should-not clutch-result-edit--error-message)
    (should-not (string-match-p "\\[invalid numeric\\]"
                                (format "%s" header-line-format)))))

(ert-deftest clutch-test-edit-json-live-validation-is-scheduled-on-idle ()
  "JSON edit buffers should defer live validation until the user goes idle."
  (with-temp-buffer
    (let (scheduled)
      (clutch--result-edit-mode 1)
      (setq-local clutch-result-edit--row-idx 0
                  clutch-result-edit--column-name "payload"
                  clutch-result-edit--column-def '(:name "payload" :type-category json)
                  clutch-result-edit--column-detail '(:name "payload" :type "json"))
      (cl-letf (((symbol-function 'run-with-idle-timer)
                 (lambda (delay _repeat fn &rest args)
                   (setq scheduled (list delay fn args))
                   :fake-timer)))
        (clutch-result-edit--schedule-validation)
        (should (= (car scheduled) clutch-insert-validation-idle-delay))
        (should (eq (cadr scheduled) #'clutch-result-edit--run-idle-validation))
        (should (equal (caddr scheduled) (list (current-buffer))))))))

(ert-deftest clutch-test-edit-finish-validates-numeric-before-stage ()
  "Edit staging should reject invalid numeric values and keep the edit buffer open."
  (let ((edit-buf (generate-new-buffer "*clutch-edit-test*"))
        staged-value
        quit-called
        err)
    (unwind-protect
        (with-current-buffer edit-buf
          (insert "xx")
          (clutch--result-edit-mode 1)
          (setq-local clutch-result-edit--column-name "impact_score"
                      clutch-result-edit--column-def '(:name "impact_score" :type-category numeric)
                      clutch-result-edit--column-detail '(:name "impact_score" :type "decimal(5,1)")
                      clutch-result--edit-callback (lambda (value) (setq staged-value value)))
          (cl-letf (((symbol-function 'quit-window)
                     (lambda (&rest _args) (setq quit-called t))))
            (setq err
                  (should-error (clutch-result-edit-finish) :type 'user-error))
            (should (string-match-p "Field impact_score expects a numeric value"
                                    (error-message-string err)))
            (should-not quit-called)
            (should-not staged-value)
            (should (buffer-live-p edit-buf))))
      (kill-buffer edit-buf))))

(ert-deftest clutch-test-edit-finish-validates-enum-before-stage ()
  "Edit staging should reject invalid enum values locally."
  (let ((edit-buf (generate-new-buffer "*clutch-edit-test*"))
        staged-value
        err)
    (unwind-protect
        (with-current-buffer edit-buf
          (insert "urgent")
          (clutch--result-edit-mode 1)
          (setq-local clutch-result-edit--column-name "severity"
                      clutch-result-edit--column-def '(:name "severity" :type-category text)
                      clutch-result-edit--column-detail
                      '(:name "severity" :type "enum('low','medium','high')")
                      clutch-result--edit-callback (lambda (value) (setq staged-value value)))
          (setq err
                (should-error (clutch-result-edit-finish) :type 'user-error))
          (should (string-match-p "Field severity must be one of: low, medium, high"
                                  (error-message-string err)))
          (should-not staged-value))
      (kill-buffer edit-buf))))

(ert-deftest clutch-test-edit-finish-validates-json-before-stage ()
  "Inline JSON edits should validate before staging."
  (let ((edit-buf (generate-new-buffer "*clutch-edit-test*"))
        staged-value
        err)
    (unwind-protect
        (with-current-buffer edit-buf
          (insert "{oops}")
          (clutch--result-edit-mode 1)
          (setq-local clutch-result-edit--column-name "payload"
                      clutch-result-edit--column-def '(:name "payload" :type-category json)
                      clutch-result-edit--column-detail '(:name "payload" :type "json")
                      clutch-result--edit-callback (lambda (value) (setq staged-value value)))
          (setq err
                (should-error (clutch-result-edit-finish) :type 'user-error))
          (should (string-match-p "Field payload expects valid JSON"
                                  (error-message-string err)))
          (should-not staged-value))
      (kill-buffer edit-buf))))

(ert-deftest clutch-test-edit-finish-allows-null-sentinel ()
  "Typing NULL in edit buffers should still stage a nil value."
  (let ((edit-buf (generate-new-buffer "*clutch-edit-test*"))
        staged-value
        quit-called)
    (unwind-protect
        (with-current-buffer edit-buf
          (insert "NULL")
          (clutch--result-edit-mode 1)
          (setq-local clutch-result-edit--column-name "impact_score"
                      clutch-result-edit--column-def '(:name "impact_score" :type-category numeric)
                      clutch-result-edit--column-detail '(:name "impact_score" :type "decimal(5,1)")
                      clutch-result--edit-callback (lambda (value) (setq staged-value value)))
          (cl-letf (((symbol-function 'quit-window)
                     (lambda (&rest _args) (setq quit-called t))))
            (clutch-result-edit-finish)
            (should quit-called)
            (should-not staged-value)))
      (kill-buffer edit-buf))))

(ert-deftest clutch-test-edit-finish-errors-when-result-buffer-is-dead ()
  "Finishing an edit should fail cleanly when the parent result buffer is gone."
  (let ((result-buf (generate-new-buffer "*clutch-result-test*"))
        edit-buf)
    (unwind-protect
        (with-current-buffer result-buf
          (clutch-result-mode)
          (setq-local clutch--result-columns '("name")
                      clutch--result-column-defs '((:name "name" :type-category text))
                      clutch--result-rows '(("before"))
                      clutch--last-query "SELECT * FROM users"
                      clutch--result-source-table "users"
                      clutch--row-identity (clutch-test--primary-row-identity
                                            "users" '("name") '(0)))
          (let ((inhibit-read-only t))
            (insert "before")
            (add-text-properties (point-min) (point-max)
                                 '(clutch-row-idx 0 clutch-col-idx 0
                                   clutch-full-value "before")))
          (goto-char (point-min))
          (cl-letf (((symbol-function 'clutch--ensure-column-details)
                     (lambda (&rest _) nil))
                    ((symbol-function 'pop-to-buffer)
                     (lambda (buf &rest _args)
                       (setq edit-buf buf)
                       buf)))
            (clutch-result-edit-cell))
          (kill-buffer result-buf)
          (with-current-buffer edit-buf
            (erase-buffer)
            (insert "after")
            (cl-letf (((symbol-function 'quit-window) #'ignore))
              (should-error (clutch-result-edit-finish) :type 'user-error))))
      (when (buffer-live-p result-buf)
        (kill-buffer result-buf))
      (when (buffer-live-p edit-buf)
        (kill-buffer edit-buf)))))

(ert-deftest clutch-test-insert-commit-validates-enum-bool-and-json-before-stage ()
  "Insert staging should reject invalid enum/bool/json values locally."
  (let ((result-buf (generate-new-buffer "*clutch-insert-result*")))
    (unwind-protect
        (let (fields-after)
          (with-current-buffer result-buf
            (setq-local clutch-connection 'fake-conn
                        clutch--result-columns '("severity" "is_ship_blocked" "postmortem")
                        clutch--result-column-defs '((:name "severity" :type-category text)
                                                     (:name "is_ship_blocked" :type-category numeric)
                                                     (:name "postmortem" :type-category json))
                        clutch--pending-inserts nil))
          (with-temp-buffer
            (insert "severity: nope\nis_ship_blocked: 7\npostmortem: not-json\n")
            (clutch-result-insert-mode 1)
            (setq-local clutch-result-insert--result-buffer result-buf
                        clutch-result-insert--table "shipping_incidents")
            (cl-letf (((symbol-function 'clutch--ensure-column-details)
                       (lambda (_conn _table)
                         (list (list :name "severity" :type "enum('low','medium')")
                               (list :name "is_ship_blocked" :type "tinyint(1)")
                               (list :name "postmortem" :type "json")))))
              (should-error (clutch-result-insert-commit) :type 'user-error)))
          (setq fields-after
                (with-current-buffer result-buf clutch--pending-inserts))
          (should (buffer-live-p result-buf))
          (should-not fields-after))
      (kill-buffer result-buf))))

(ert-deftest clutch-test-insert-commit-validates-temporal-before-stage ()
  "Insert staging should reject invalid date/time/datetime values locally."
  (let ((result-buf (generate-new-buffer "*clutch-insert-result*")))
    (unwind-protect
        (let (fields-after err-msg)
          (with-current-buffer result-buf
            (setq-local clutch-connection 'fake-conn
                        clutch--result-columns '("opened_at" "due_on" "starts_at")
                        clutch--result-column-defs '((:name "opened_at" :type-category datetime)
                                                     (:name "due_on" :type-category date)
                                                     (:name "starts_at" :type-category time))
                        clutch--pending-inserts nil))
          (with-temp-buffer
            (insert "opened_at: ss\ndue_on: 2026-02-30\nstarts_at: 25:61\n")
            (clutch-result-insert-mode 1)
            (setq-local clutch-result-insert--result-buffer result-buf
                        clutch-result-insert--table "shipping_incidents")
            (cl-letf (((symbol-function 'clutch--ensure-column-details)
                       (lambda (_conn _table)
                         (list (list :name "opened_at" :type "datetime")
                               (list :name "due_on" :type "date")
                               (list :name "starts_at" :type "time")))))
              (setq err-msg
                    (should-error (clutch-result-insert-commit)
                                  :type 'user-error))
              (should (string-match-p "Field opened_at expects YYYY-MM-DD HH:MM\\[:SS\\]"
                                      (error-message-string err-msg)))))
          (setq fields-after
                (with-current-buffer result-buf clutch--pending-inserts))
          (should-not fields-after))
      (kill-buffer result-buf))))

(ert-deftest clutch-test-insert-commit-validates-numeric-before-stage ()
  "Insert staging should reject invalid numeric values locally."
  (let ((result-buf (generate-new-buffer "*clutch-insert-result*")))
    (unwind-protect
        (let (fields-after err-msg)
          (with-current-buffer result-buf
            (setq-local clutch-connection 'fake-conn
                        clutch--result-columns '("impact_score")
                        clutch--result-column-defs '((:name "impact_score" :type-category numeric))
                        clutch--pending-inserts nil))
          (with-temp-buffer
            (insert "impact_score: xx\n")
            (clutch-result-insert-mode 1)
            (setq-local clutch-result-insert--result-buffer result-buf
                        clutch-result-insert--table "shipping_incidents")
            (cl-letf (((symbol-function 'clutch--ensure-column-details)
                       (lambda (_conn _table)
                         (list (list :name "impact_score" :type "decimal(5,1)")))))
              (setq err-msg
                    (should-error (clutch-result-insert-commit)
                                  :type 'user-error))
              (should (string-match-p "Field impact_score expects a numeric value"
                                      (error-message-string err-msg)))))
          (setq fields-after
                (with-current-buffer result-buf clutch--pending-inserts))
          (should-not fields-after))
      (kill-buffer result-buf))))

;;;; Edit — JSON sub-editor

(ert-deftest clutch-test-json-editor-mode-falls-back-when-json-ts-errors ()
  "JSON editors should tolerate missing tree-sitter grammars."
  (let (selected-mode)
    (with-temp-buffer
      (cl-letf (((symbol-function 'json-ts-mode)
                 (lambda () (error "missing JSON grammar")))
                ((symbol-function 'js-mode)
                 (lambda () (setq selected-mode 'js-mode))))
        (clutch-result-insert--json-editor-mode)))
    (should (eq selected-mode 'js-mode))))

(ert-deftest clutch-test-insert-json-editor-save-roundtrip ()
  "Saving a JSON child editor should write compact JSON back to the insert field."
  (let ((result-buf (generate-new-buffer "*clutch-insert-result*"))
        (editor-buf nil))
    (unwind-protect
        (progn
          (with-current-buffer result-buf
            (setq-local clutch-connection 'fake-conn
                        clutch--result-columns '("postmortem")
                        clutch--result-column-defs '((:name "postmortem" :type-category json))))
          (with-temp-buffer
            (insert "postmortem: \n")
            (clutch-result-insert-mode 1)
            (setq-local clutch-result-insert--result-buffer result-buf
                        clutch-result-insert--table "shipping_incidents")
            (cl-letf (((symbol-function 'clutch--ensure-column-details)
                       (lambda (_conn _table)
                         (list (list :name "postmortem" :type "json"))))
                      ((symbol-function 'pop-to-buffer)
                       (lambda (buf &rest _args)
                         (setq editor-buf buf)
                         buf)))
              (clutch-result-insert-edit-json-field))
            (with-current-buffer editor-buf
              (erase-buffer)
              (insert "{\n  \"severity\": \"high\",\n  \"ship_blocked\": true\n}")
              (cl-letf (((symbol-function 'clutch--ensure-column-details)
                         (lambda (_conn _table)
                           (list (list :name "postmortem" :type "json"))))
                        ((symbol-function 'quit-window) (lambda (&rest _args) nil))
                        ((symbol-function 'pop-to-buffer) (lambda (buf &rest _args) buf)))
                (clutch-result-insert-json-finish)))
            (should (equal (buffer-string)
                           "postmortem: {\"severity\":\"high\",\"ship_blocked\":true}\n"))))
      (kill-buffer result-buf)
      (when (buffer-live-p editor-buf)
        (kill-buffer editor-buf)))))

(ert-deftest clutch-test-insert-json-editor-cancel-keeps-parent-value ()
  "Cancelling the JSON child editor should leave the parent field unchanged."
  (let ((result-buf (generate-new-buffer "*clutch-insert-result*"))
        (editor-buf nil))
    (unwind-protect
        (progn
          (with-current-buffer result-buf
            (setq-local clutch-connection 'fake-conn
                        clutch--result-columns '("postmortem")
                        clutch--result-column-defs '((:name "postmortem" :type-category json))))
          (with-temp-buffer
            (insert "postmortem: {\"ok\":true}\n")
            (clutch-result-insert-mode 1)
            (setq-local clutch-result-insert--result-buffer result-buf
                        clutch-result-insert--table "shipping_incidents")
            (cl-letf (((symbol-function 'clutch--ensure-column-details)
                       (lambda (_conn _table)
                         (list (list :name "postmortem" :type "json"))))
                      ((symbol-function 'pop-to-buffer)
                       (lambda (buf &rest _args)
                         (setq editor-buf buf)
                         buf)))
              (clutch-result-insert-edit-json-field))
            (with-current-buffer editor-buf
              (erase-buffer)
              (insert "{\"ok\":false}")
              (cl-letf (((symbol-function 'quit-window) (lambda (&rest _args) nil))
                        ((symbol-function 'pop-to-buffer) (lambda (buf &rest _args) buf)))
                (clutch-result-insert-json-cancel)))
            (should (equal (buffer-string)
                           "postmortem: {\"ok\":true}\n"))))
      (kill-buffer result-buf)
      (when (buffer-live-p editor-buf)
        (kill-buffer editor-buf)))))

(ert-deftest clutch-test-edit-json-field-roundtrip ()
  "JSON edit sub-buffer should save normalized contents back to the parent edit buffer."
  (let ((parent-buf (generate-new-buffer "*clutch-edit-parent*")))
    (unwind-protect
        (with-current-buffer parent-buf
          (insert "{\"a\":1}")
          (clutch--result-edit-mode 1)
          (setq-local clutch-result-edit--column-name "payload"
                      clutch-result-edit--column-def '(:name "payload" :type-category json)
                      clutch-result-edit--column-detail '(:name "payload" :type "json"))
          (cl-letf (((symbol-function 'pop-to-buffer)
                     (lambda (buf &rest _args) buf)))
            (let ((json-buf (clutch-result-edit-json-field)))
              (with-current-buffer json-buf
                (erase-buffer)
                (insert "{\"a\":2}")
                (clutch-result-edit-json-finish))
              (should (equal (with-current-buffer parent-buf (buffer-string))
                             "{\"a\":2}")))))
      (kill-buffer parent-buf))))

;;;; Export — dispatch and content

(ert-deftest clutch-test-export-command-writes-selected-format-content ()
  "Export command should write the chosen format through the real export path."
  (dolist (case '(("csv-copy" clipboard "id,name\n1,\"a,b\"\n")
                  ("csv-file" file "id,name\n1,\"a,b\"\n")
                  ("insert-copy" clipboard
                   "INSERT INTO \"users\" (\"id\", \"name\") VALUES (1, 'a,b');\n")
                  ("insert-file" file
                   "INSERT INTO \"users\" (\"id\", \"name\") VALUES (1, 'a,b');\n")
                  ("update-copy" clipboard
                   "UPDATE \"users\" SET \"name\" = 'a,b' WHERE \"id\" = 1\n")
                  ("update-file" file
                   "UPDATE \"users\" SET \"name\" = 'a,b' WHERE \"id\" = 1\n")))
    (pcase-let ((`(,choice ,target ,expected) case))
      (ert-info ((format "export choice: %s" choice))
        (let ((path (make-temp-file "clutch-export-"))
              (kill-ring nil)
              (kill-ring-yank-pointer nil))
          (unwind-protect
              (with-temp-buffer
                (setq-local clutch-connection 'fake-conn
                            clutch--result-source-table "users"
                            clutch--result-columns '("id" "name")
                            clutch--last-query "SELECT id, name FROM users"
                            clutch--result-column-defs '((:name "id" :type-category numeric)
                                                         (:name "name" :type-category text))
                            clutch--row-identity
                            (clutch-test--primary-row-identity
                             "users" '("id") '(0)))
                (cl-letf (((symbol-function 'completing-read)
                           (lambda (prompt choices &rest _args)
                             (cond
                              ((string-prefix-p "Export format:" prompt)
                               (should (member choice choices))
                               choice)
                              ((string-prefix-p "CSV encoding" prompt)
                               "utf-8")
                              (t
                               (ert-fail (format "Unexpected prompt: %s" prompt))))))
                          ((symbol-function 'read-file-name)
                           (lambda (&rest _args) path))
                          ((symbol-function 'clutch-result--collect-all-export-rows)
                           (lambda () '((1 "a,b"))))
                          ((symbol-function 'clutch--ensure-column-details)
                           (lambda (_conn _table &optional _strict)
                             (list (list :name "id")
                                   (list :name "name"))))
                          ((symbol-function 'clutch-db-escape-identifier)
                           (lambda (_conn s) (format "\"%s\"" s)))
                          ((symbol-function 'clutch-db-escape-literal)
                           (lambda (_conn s) (format "'%s'" s))))
                  (clutch-result-export)
                  (should
                   (equal (if (eq target 'clipboard)
                              (current-kill 0)
                            (with-temp-buffer
                              (insert-file-contents path)
                              (buffer-string)))
                          expected))))
            (ignore-errors (delete-file path))))))))

(ert-deftest clutch-test-csv-content-escaping ()
  "CSV content should include header and escaped values."
  (with-temp-buffer
    (setq-local clutch--result-columns '("id" "name"))
    (let ((csv (clutch--export-csv-content '((1 "a,b") (2 "x\"y")))))
      (should (string-match-p "^id,name\n" csv))
      (should (string-match-p "1,\"a,b\"" csv))
      (should (string-match-p "2,\"x\"\"y\"" csv)))))

(ert-deftest clutch-test-insert-content-builds-full-row-sql ()
  "INSERT export content should build SQL from the ROWS argument."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn
                clutch--result-columns '("id" "name")
                clutch--result-rows '((999 "current-page-only"))
                clutch--last-query "SELECT id, name FROM users")
    (cl-letf (((symbol-function 'clutch-db-escape-identifier)
               (lambda (_conn s) (format "\"%s\"" s)))
              ((symbol-function 'clutch-db-escape-literal)
               (lambda (_conn s) (format "'%s'" s))))
      (should (equal (clutch--export-insert-content '((1 "a") (2 "b")))
                     (concat
                      "INSERT INTO \"users\" (\"id\", \"name\") VALUES (1, 'a');\n"
                      "INSERT INTO \"users\" (\"id\", \"name\") VALUES (2, 'b');\n")))
      (should-not (string-match-p "current-page-only"
                                  (clutch--export-insert-content '((1 "a"))))))))

(ert-deftest clutch-test-update-content-builds-full-row-sql ()
  "UPDATE export content should build SQL from the ROWS argument."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn
                clutch--result-source-table "users"
                clutch--result-columns '("id" "name")
                clutch--result-column-defs '((:name "id" :type-category numeric)
                                             (:name "name" :type-category text))
                clutch--result-rows '((999 "current-page-only"))
                clutch--row-identity (clutch-test--primary-row-identity
                                      "users" '("id") '(0)))
    (cl-letf (((symbol-function 'clutch--ensure-column-details)
               (lambda (_conn _table &optional _strict)
                 (list (list :name "id")
                       (list :name "name"))))
              ((symbol-function 'clutch-db-escape-identifier)
               (lambda (_conn s) (format "\"%s\"" s)))
              ((symbol-function 'clutch-db-escape-literal)
               (lambda (_conn s) (format "'%s'" s))))
      (should (equal (clutch--export-update-content '((1 "a") (2 "b")))
                     (concat
                      "UPDATE \"users\" SET \"name\" = 'a' WHERE \"id\" = 1\n"
                      "UPDATE \"users\" SET \"name\" = 'b' WHERE \"id\" = 2\n")))
      (should-not (string-match-p "current-page-only"
                                  (clutch--export-update-content '((1 "a"))))))))

(ert-deftest clutch-test-simple-insert-source-table-rejects-joined-query ()
  "Joined result queries should not pretend one table is the INSERT target."
  (with-temp-buffer
    (setq-local clutch--last-query
                "SELECT u.id, p.title FROM users u JOIN posts p ON p.user_id = u.id")
    (should (equal (clutch--insert-target-table) "MY_TABLE"))))

(ert-deftest clutch-test-copy-rows-as-insert-uses-placeholder-for-ambiguous-source ()
  "INSERT copy should use a placeholder table for ambiguous result queries."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn
                clutch--result-columns '("id")
                clutch--result-rows '((1))
                clutch--last-query
                "SELECT u.id FROM users u JOIN posts p ON p.user_id = u.id")
    (cl-letf (((symbol-function 'clutch--cell-at-point)
               (lambda () (list 0 0 1)))
              ((symbol-function 'clutch-db-escape-identifier)
               (lambda (_conn s) s))
              ((symbol-function 'clutch-db-escape-literal)
               (lambda (_conn s) (format "'%s'" s))))
      (clutch-result--copy-rows 'insert)
      (should (equal (current-kill 0) "INSERT INTO MY_TABLE (id) VALUES (1);")))))

(ert-deftest clutch-test-insert-content-uses-placeholder-for-ambiguous-source ()
  "INSERT export content should use `MY_TABLE' for ambiguous result queries."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn
                clutch--result-columns '("id" "name")
                clutch--last-query
                "SELECT u.id, p.name FROM users u JOIN posts p ON p.user_id = u.id")
    (cl-letf (((symbol-function 'clutch-db-escape-identifier)
               (lambda (_conn s) s))
              ((symbol-function 'clutch-db-escape-literal)
               (lambda (_conn s) (format "'%s'" s))))
      (should (equal (clutch--export-insert-content '((1 "a") (2 "b")))
                     (concat
                      "INSERT INTO MY_TABLE (id, name) VALUES (1, 'a');\n"
                      "INSERT INTO MY_TABLE (id, name) VALUES (2, 'b');\n"))))))

(ert-deftest clutch-test-copy-update-with-region-uses-region-rectangle ()
  "UPDATE copy should generate SQL from rectangle row/column selection."
  (with-temp-buffer
    (let (kill-ring kill-ring-yank-pointer)
      (setq-local clutch-connection 'fake-conn
                  clutch--result-source-table "users"
                  clutch--result-columns '("id" "name" "status")
                  clutch--result-column-defs '((:name "id" :type-category numeric)
                                               (:name "name" :type-category text)
                                               (:name "status" :type-category text))
                  clutch--row-identity (clutch-test--primary-row-identity
                                        "users" '("id") '(0))
                  clutch--result-rows '((1 "a" "new")
                                        (2 "b" "done")))
      (cl-letf (((symbol-function 'use-region-p) (lambda () t))
                ((symbol-function 'clutch-result--region-rectangle-indices)
                 (lambda () '((0 1) . (1 2))))
                ((symbol-function 'clutch--ensure-column-details)
                 (lambda (_conn _table &optional _strict)
                   (list (list :name "id")
                         (list :name "name")
                         (list :name "status"))))
                ((symbol-function 'clutch-db-escape-identifier)
                 (lambda (_conn s) (format "\"%s\"" s)))
                ((symbol-function 'clutch-db-escape-literal)
                 (lambda (_conn s) (format "'%s'" s))))
        (clutch-result--copy-rows 'update)
        (should
         (equal (current-kill 0)
                (concat
                 "UPDATE \"users\" SET \"name\" = 'a', \"status\" = 'new' WHERE \"id\" = 1\n"
                 "UPDATE \"users\" SET \"name\" = 'b', \"status\" = 'done' WHERE \"id\" = 2")))))))

(ert-deftest clutch-test-copy-update-without-region-copies-current-cell ()
  "UPDATE copy without region should generate SQL for the current cell."
  (with-temp-buffer
    (let (kill-ring kill-ring-yank-pointer)
      (setq-local clutch-connection 'fake-conn
                  clutch--result-source-table "users"
                  clutch--result-columns '("id" "name")
                  clutch--result-column-defs '((:name "id" :type-category numeric)
                                               (:name "name" :type-category text))
                  clutch--row-identity (clutch-test--primary-row-identity
                                        "users" '("id") '(0))
                  clutch--result-rows '((1 "a")))
      (cl-letf (((symbol-function 'use-region-p) (lambda () nil))
                ((symbol-function 'clutch--cell-at-point)
                 (lambda () (list 0 1 "a")))
                ((symbol-function 'clutch--ensure-column-details)
                 (lambda (_conn _table &optional _strict)
                   (list (list :name "id")
                         (list :name "name"))))
                ((symbol-function 'clutch-db-escape-identifier)
                 (lambda (_conn s) (format "\"%s\"" s)))
                ((symbol-function 'clutch-db-escape-literal)
                 (lambda (_conn s) (format "'%s'" s))))
        (clutch-result--copy-rows 'update)
        (should (equal (current-kill 0)
                       "UPDATE \"users\" SET \"name\" = 'a' WHERE \"id\" = 1"))))))

(ert-deftest clutch-test-copy-update-with-filter-uses-visible-row ()
  "UPDATE copy should use the filtered display row for visible indices."
  (with-temp-buffer
    (setq-local clutch--result-columns '("id" "name")
                clutch--result-rows '((1 "alpha") (2 "beta"))
                clutch--filtered-rows '((2 "beta")))
    (cl-letf (((symbol-function 'use-region-p) (lambda () nil))
              ((symbol-function 'clutch--cell-at-point)
               (lambda () (list 0 1 "beta")))
              ((symbol-function 'clutch-result--build-update-statements-for-rows)
               (lambda (rows col-indices op)
                 (should (equal rows '((2 "beta"))))
                 (should (equal col-indices '(1)))
                 (should (equal op "copy UPDATE SQL"))
                 '("UPDATE users SET name = 'beta' WHERE id = 2"))))
      (clutch-result--copy-rows 'update))))

(ert-deftest clutch-test-build-csv-lines-with-filter-uses-visible-row ()
  "CSV copy should use filtered display rows for visible row indices."
  (with-temp-buffer
    (setq-local clutch--result-columns '("id" "name")
                clutch--result-rows '((1 "alpha") (2 "beta"))
                clutch--filtered-rows '((2 "beta")))
    (should (equal (clutch--csv-lines-for-rows
                    (clutch-result--rows-for-display-indices '(0)) '(1))
                   '("name" "beta")))))

(ert-deftest clutch-test-build-insert-statements-with-filter-uses-visible-row ()
  "INSERT copy should use filtered display rows for visible row indices."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn
                clutch--result-columns '("id" "name")
                clutch--result-rows '((1 "alpha") (2 "beta"))
                clutch--filtered-rows '((2 "beta")))
    (cl-letf (((symbol-function 'clutch-db-escape-identifier)
               (lambda (_conn s) s))
              ((symbol-function 'clutch-db-escape-literal)
               (lambda (_conn s) (format "'%s'" s))))
      (should (equal (clutch-result--build-insert-statements-for-rows
                      (clutch-result--rows-for-display-indices '(0))
                      '(1) "users")
                     '("INSERT INTO users (name) VALUES ('beta');"))))))

(ert-deftest clutch-test-copy-update-errors-when-only-pk-column-is-selected ()
  "UPDATE copy should reject selections that contain only primary key columns."
  (with-temp-buffer
    (setq-local clutch--result-columns '("id" "name")
                clutch--result-column-defs '((:name "id" :type-category numeric)
                                             (:name "name" :type-category text))
                clutch--result-source-table "users"
                clutch--row-identity (clutch-test--primary-row-identity
                                      "users" '("id") '(0)))
    (let ((err (should-error
                (clutch-result--build-update-statements-for-rows
                 '((1 "a")) '(0) "copy UPDATE SQL")
                :type 'user-error)))
      (should (string-match-p
               "Cannot copy UPDATE SQL: no writable source columns selected"
               (error-message-string err))))))

(ert-deftest clutch-test-copy-update-errors-on-non-source-columns ()
  "UPDATE copy should reject alias or computed columns."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn
                clutch--result-columns '("id" "name" "computed_total")
                clutch--result-column-defs '((:name "id" :type-category numeric)
                                             (:name "name" :type-category text)
                                             (:name "computed_total" :type-category numeric))
                clutch--result-source-table "users"
                clutch--row-identity (clutch-test--primary-row-identity
                                      "users" '("id") '(0)))
    (cl-letf (((symbol-function 'clutch--ensure-column-details)
               (lambda (_conn _table &optional _strict)
                 (list (list :name "id")
                       (list :name "name")))))
      (let ((err (should-error
                  (clutch-result--build-update-statements-for-rows
                   '((1 "alice" 42)) '(0 1 2) "copy UPDATE SQL")
                  :type 'user-error)))
        (should (string-match-p
                 "Cannot copy UPDATE SQL: selected columns are not writable source columns: computed_total"
                 (error-message-string err)))))))

(ert-deftest clutch-test-copy-update-errors-on-generated-source-columns ()
  "UPDATE copy should reject generated source columns."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn
                clutch--result-columns '("id" "generated_name")
                clutch--result-column-defs '((:name "id" :type-category numeric)
                                             (:name "generated_name" :type-category text))
                clutch--result-source-table "users"
                clutch--row-identity (clutch-test--primary-row-identity
                                      "users" '("id") '(0)))
    (cl-letf (((symbol-function 'clutch--ensure-column-details)
               (lambda (_conn _table &optional _strict)
                 (list (list :name "id")
                       (list :name "generated_name" :generated t)))))
      (let ((err (should-error
                  (clutch-result--build-update-statements-for-rows
                   '((1 "alice")) '(0 1) "copy UPDATE SQL")
                  :type 'user-error)))
        (should (string-match-p
                 "Cannot copy UPDATE SQL: selected columns are not writable source columns: generated_name"
                 (error-message-string err)))))))

(ert-deftest clutch-test-copy-pending-sql-copies-current-batch ()
  "Staged SQL copy should mirror the staged commit batch."
  (with-temp-buffer
    (let (copied)
      (setq-local clutch--pending-inserts '(a)
                  clutch--pending-edits '(b)
                  clutch--pending-deletes '(c))
      (cl-letf (((symbol-function 'clutch-result--build-pending-insert-statements)
                 (lambda () '(("INSERT INTO t VALUES (1)" . nil))))
                ((symbol-function 'clutch-result--build-update-statements)
                 (lambda () '(("UPDATE t SET name='' WHERE id=1" . nil))))
                ((symbol-function 'clutch-result--build-pending-delete-statements)
                 (lambda () '(("DELETE FROM t WHERE id=1" . nil))))
                ((symbol-function 'kill-new)
                 (lambda (text) (setq copied text))))
        (clutch-result-copy-pending-sql)
        (should (equal copied
                       "INSERT INTO t VALUES (1);\nUPDATE t SET name='' WHERE id=1;\nDELETE FROM t WHERE id=1;\n"))))))

(ert-deftest clutch-test-save-pending-sql-writes-current-batch ()
  "Staged SQL save should write the staged commit batch to disk."
  (let ((path (make-temp-file "clutch-pending-" nil ".sql")))
    (unwind-protect
        (with-temp-buffer
          (setq-local clutch--pending-edits '(b))
          (cl-letf (((symbol-function 'clutch-result--build-update-statements)
                     (lambda () '(("UPDATE t SET name='' WHERE id=1" . nil))))
                    ((symbol-function 'read-file-name)
                     (lambda (&rest _args) path)))
            (clutch-result-save-pending-sql)
            (should (equal (with-temp-buffer
                             (insert-file-contents path)
                             (buffer-string))
                           "UPDATE t SET name='' WHERE id=1;\n"))))
      (delete-file path))))

;;;; Agent context copy

(defun clutch-test--with-agent-context-stubs (body)
  "Call BODY with deterministic metadata for agent-context copy tests."
  (let ((clutch--column-details-cache (make-hash-table :test 'equal))
        (clutch--column-details-status-cache (make-hash-table :test 'equal))
        (clutch--table-comment-cache (make-hash-table :test 'equal))
        (clutch--object-cache (make-hash-table :test 'equal)))
    (cl-letf (((symbol-function 'clutch--ensure-connection)
               #'ignore)
              ((symbol-function 'clutch--connection-key)
               (lambda (_conn) "app@db.local:5432/app"))
              ((symbol-function 'clutch-db-display-name)
               (lambda (_conn) "PostgreSQL"))
              ((symbol-function 'clutch-db-database)
               (lambda (_conn) "app"))
              ((symbol-function 'clutch-db-current-schema)
               (lambda (_conn) "public"))
              ((symbol-function 'clutch-db-table-comment)
               (lambda (_conn table)
                 (when (string= table "users")
                   "application users")))
              ((symbol-function 'clutch-db-column-details)
               (lambda (_conn table)
                 (when (string= table "users")
                   (list (list :name "id" :type "bigint"
                               :nullable nil :primary-key t)
                         (list :name "email" :type "text"
                               :nullable nil :comment "login email"
                               :foreign-key '(:ref-table "orgs"
                                             :ref-column "id"))))))
              ((symbol-function 'clutch--object-related-entries)
               (lambda (_conn entry type)
                 (when (and (string= (plist-get entry :name) "users")
                            (string= type "INDEX"))
                   (list (list :name "users_email_idx" :type "INDEX"
                               :target-table "users" :unique t))))))
      (funcall body))))

(defun clutch-test--copy-agent-context ()
  "Run `clutch-copy-context-for-agent' and return copied text."
  (let (copied)
    (clutch-test--with-agent-context-stubs
     (lambda ()
       (cl-letf (((symbol-function 'kill-new)
                  (lambda (text) (setq copied text))))
         (clutch-copy-context-for-agent))))
    copied))

(ert-deftest clutch-test-copy-context-for-agent-from-query-console ()
  "Agent context copy should use the public query-console command path."
  (with-temp-buffer
    (clutch-mode)
    (insert "SELECT id, email FROM users WHERE active = 1")
    (setq-local clutch-connection 'fake-conn)
    (let ((copied (clutch-test--copy-agent-context)))
      (should (string-match-p "# Clutch database context" copied))
      (should (string-match-p "- Backend: PostgreSQL" copied))
      (should (string-match-p "SELECT id, email FROM users WHERE active = 1" copied))
      (should (string-match-p "## Table: users" copied))
      (should (string-match-p "users (TABLE)" copied))
      (should (string-match-p "Comment\n  application users" copied))
      (should (string-match-p "Columns (2)" copied))
      (should (string-match-p "id[[:space:]]+bigint[[:space:]]+NOT NULL, PK" copied))
      (should (string-match-p
               "email[[:space:]]+text[[:space:]]+NOT NULL, FK -> orgs.id, login email"
               copied))
      (should (string-match-p "Indexes (1)" copied))
      (should (string-match-p "users_email_idx[[:space:]]+UNIQUE" copied))
      (should-not (string-match-p "Row identity candidates" copied)))))

(ert-deftest clutch-test-result-mode-k-copies-agent-context ()
  "The documented result-mode k binding should copy agent context."
  (should (eq (lookup-key clutch-result-mode-map "k")
              #'clutch-copy-context-for-agent)))

(ert-deftest clutch-test-copy-context-for-agent-at-end-of-semicolon-statement ()
  "Agent context copy should use the previous statement after a trailing semicolon."
  (with-temp-buffer
    (clutch-mode)
    (insert "SELECT id, email FROM users WHERE active = 1;")
    (setq-local clutch-connection 'fake-conn)
    (let ((copied (clutch-test--copy-agent-context)))
      (should (string-match-p "SELECT id, email FROM users WHERE active = 1" copied))
      (should (string-match-p "## Table: users" copied)))))

(ert-deftest clutch-test-copy-context-for-agent-from-result-buffer-includes-sample ()
  "Agent context copy should include effective result SQL and current sample rows."
  (let ((clutch-agent-context-max-result-rows 1)
        copied)
    (with-temp-buffer
      (clutch-result-mode)
      (setq-local clutch-connection 'fake-conn
                  clutch--base-query "SELECT id, email FROM users"
                  clutch--where-filter "active = 1"
                  clutch--result-source-table "users"
                  clutch--result-columns '("clutch__rid_0" "id" "email")
                  clutch--result-column-defs '((:name "clutch__rid_0" :hidden t)
                                               (:name "id")
                                               (:name "email"))
                  clutch--result-rows (list (vector "rid-1" 1 "ada@example.com")
                                            (vector "rid-2" 2 "bob@example.com")))
      (cl-letf (((symbol-function 'clutch-db-apply-where)
                 (lambda (_conn sql filter)
                   (format "SELECT * FROM (%s) AS clutch_q WHERE %s" sql filter))))
        (setq copied (clutch-test--copy-agent-context))))
    (should (string-match-p
             "SELECT \\* FROM (SELECT id, email FROM users) AS clutch_q WHERE active = 1"
             copied))
    (should (string-match-p "## Result sample" copied))
    (should (string-match-p "Showing 1 of 2 visible rows" copied))
    (should (string-match-p "id\temail" copied))
    (should (string-match-p "1\tada@example.com" copied))
    (should-not (string-match-p "clutch__rid_0" copied))
    (should-not (string-match-p "rid-1" copied))
    (should-not (string-match-p "bob@example.com" copied))))

(ert-deftest clutch-test-copy-context-for-agent-from-query-console-includes-last-result-sample ()
  "Agent context copy from a query console should include its latest result sample."
  (let ((clutch-agent-context-max-result-rows 1)
        (result-buf (generate-new-buffer " *clutch-agent-result*"))
        copied)
    (unwind-protect
        (with-temp-buffer
          (clutch-mode)
          (insert "SELECT id, email FROM users")
          (setq-local clutch-connection 'fake-conn
                      clutch--last-result-buffer result-buf)
          (with-current-buffer result-buf
            (clutch-result-mode)
            (setq-local clutch-connection 'fake-conn
                        clutch--base-query "SELECT id, email FROM users"
                        clutch--result-columns '("id" "email")
                        clutch--result-rows (list (vector 1 "ada@example.com")
                                                  (vector 2 "bob@example.com"))))
          (setq copied (clutch-test--copy-agent-context))
          (should (string-match-p "## Result sample" copied))
          (should (string-match-p "Showing 1 of 2 visible rows" copied))
          (should (string-match-p "id\temail" copied))
          (should (string-match-p "1\tada@example.com" copied))
          (should-not (string-match-p "bob@example.com" copied)))
      (when (buffer-live-p result-buf)
        (kill-buffer result-buf)))))

;;;; Aggregate and copy

(ert-deftest clutch-test-selected-row-indices-priority ()
  "Selection priority should be region > current row."
  (with-temp-buffer
    (cl-letf (((symbol-function 'use-region-p) (lambda () t))
              ((symbol-function 'clutch--rows-in-region)
               (lambda (_beg _end) '(2 3)))
              ((symbol-function 'clutch--row-idx-at-line)
               (lambda () 1))
              ((symbol-function 'region-beginning) (lambda () 10))
              ((symbol-function 'region-end) (lambda () 20)))
      (should (equal (clutch--selected-row-indices) '(2 3)))))
  (with-temp-buffer
    (cl-letf (((symbol-function 'use-region-p) (lambda () nil))
              ((symbol-function 'clutch--row-idx-at-line)
               (lambda () 4)))
      (should (equal (clutch--selected-row-indices) '(4))))))

(ert-deftest clutch-test-aggregate-current-column-without-region ()
  "Aggregate should use current cell when region is inactive."
  (with-temp-buffer
    (let (kill-ring kill-ring-yank-pointer)
      (setq-local clutch--result-columns '("id" "score"))
      (setq-local clutch--result-rows '((1 "1.5") (2 "2.5") (3 "x") (4 4)))
      (cl-letf (((symbol-function 'use-region-p) (lambda () nil))
                ((symbol-function 'clutch--cell-at-point)
                 (lambda () '(1 1 "2.5"))))
        (clutch-result-aggregate)
        (let ((summary (current-kill 0)))
          (should (string-match-p "Aggregate \\[score\\]" summary))
          (should (string-match-p "sum=2.5" summary))
          (should (string-match-p "avg=2.5" summary))
          (should (string-match-p "\\[rows=1 cells=1 skipped=0\\]" summary)))))))

(ert-deftest clutch-test-aggregate-region-multi-column-aggregates-all-columns ()
  "Aggregate should summarize all selected cells as one result."
  (with-temp-buffer
    (let (kill-ring kill-ring-yank-pointer)
      (setq-local clutch--result-columns '("id" "a" "b"))
      (setq-local clutch--result-rows '((1 10 20) (2 11 21)))
      (cl-letf (((symbol-function 'use-region-p) (lambda () t))
                ((symbol-function 'clutch-result--region-rectangle-indices)
                 (lambda () '((0 1) 1 2))))
        (clutch-result-aggregate)
        (let ((summary (current-kill 0)))
          (should (string-match-p "Aggregate \\[selection\\]" summary))
          (should (string-match-p "sum=62" summary))
          (should (string-match-p "avg=15.5" summary))
          (should (string-match-p "\\[rows=2 cells=4 skipped=0\\]" summary)))))))

(ert-deftest clutch-test-aggregate-region-single-column ()
  "Aggregate should support rectangular region for one selected column."
  (with-temp-buffer
    (let (kill-ring kill-ring-yank-pointer)
      (setq-local clutch--result-columns '("id" "score"))
      (setq-local clutch--result-rows '((1 "1") (2 "2") (3 "3")))
      (cl-letf (((symbol-function 'use-region-p) (lambda () t))
                ((symbol-function 'clutch-result--region-rectangle-indices)
                 (lambda () '((0 2) 1)))
                ((symbol-function 'clutch--cell-at-point)
                 (lambda () '(0 1 "1"))))
        (clutch-result-aggregate)
        (let ((summary (current-kill 0)))
          (should (string-match-p "Aggregate \\[score\\]" summary))
          (should (string-match-p "sum=4" summary))
          (should (string-match-p "avg=2" summary))
          (should (string-match-p "\\[rows=2 cells=2 skipped=0\\]" summary)))))))

(ert-deftest clutch-test-aggregate-with-prefix-refines-region ()
  "Prefix-arg aggregate should use refined rectangle selection."
  (with-temp-buffer
    (let (kill-ring kill-ring-yank-pointer)
      (setq-local clutch--result-columns '("id" "score"))
      (setq-local clutch--result-rows '((1 "1") (2 "2") (3 "3")))
      (cl-letf (((symbol-function 'use-region-p) (lambda () t))
                ((symbol-function 'clutch-result--region-rectangle-indices)
                 (lambda () '((0 1 2) . (1))))
                ((symbol-function 'clutch-result--start-refine)
                 (lambda (_rect callback)
                   (funcall callback '((0 2) . (1)))))
                ((symbol-function 'clutch--cell-at-point)
                 (lambda () '(0 1 "1"))))
        (clutch-result-aggregate t)
        (let ((summary (current-kill 0)))
          (should (string-match-p "sum=4" summary))
          (should (string-match-p "\\[rows=2 cells=2 skipped=0\\]" summary)))))))

(ert-deftest clutch-test-down-cell-keeps-region-active ()
  "Row navigation should keep region active for selection workflows."
  (with-temp-buffer
    (let ((deactivate-mark t))
      (cl-letf (((symbol-function 'use-region-p) (lambda () t))
                ((symbol-function 'clutch--col-idx-at-point) (lambda () 1))
                ((symbol-function 'get-text-property)
                 (lambda (_pos prop &optional _object)
                   (when (eq prop 'clutch-row-idx) 2)))
                ((symbol-function 'clutch--goto-cell) (lambda (&rest _args) nil)))
        (clutch-result-down-cell)
        (should-not deactivate-mark)))))

(ert-deftest clutch-test-region-cells-rectangle ()
  "Region cell extraction should use rectangular cell bounds."
  (with-temp-buffer
    (setq-local clutch--result-rows
                '((r0c0 r0c1 r0c2)
                  (r1c0 r1c1 r1c2)
                  (r2c0 r2c1 r2c2)))
    (cl-letf (((symbol-function 'region-beginning) (lambda () 10))
              ((symbol-function 'region-end) (lambda () 20))
              ((symbol-function 'clutch--cell-at-or-near)
               (lambda (pos)
                 (if (= pos 10) '(0 1 nil) '(2 1 nil)))))
      (should (equal (clutch-result--region-cells)
                     '((0 1 r0c1)
                       (1 1 r1c1)
                       (2 1 r2c1)))))))

(ert-deftest clutch-test-yank-cell-with-region-copies-region-cells ()
  "Yank cell should copy region cells as TSV when region is active."
  (with-temp-buffer
    (let (kill-ring kill-ring-yank-pointer)
      (cl-letf (((symbol-function 'use-region-p)
                 (lambda () t))
                ((symbol-function 'region-beginning)
                 (lambda () 10))
                ((symbol-function 'region-end)
                 (lambda () 20))
                ((symbol-function 'clutch-result--region-cells)
                 (lambda ()
                   '((0 0 1) (0 2 "shanghai") (1 1 "bob")))))
        (clutch-result-copy 'tsv)
        (should (equal (current-kill 0) "1\tshanghai\nbob"))))))

(ert-deftest clutch-test-yank-cell-without-region-copies-point-cell ()
  "Yank cell should ignore region logic when region is not active."
  (with-temp-buffer
    (let (kill-ring kill-ring-yank-pointer)
      (cl-letf (((symbol-function 'use-region-p)
                 (lambda () nil))
                ((symbol-function 'clutch--cell-at-point)
                 (lambda () '(2 3 "alice"))))
        (clutch-result-copy 'tsv)
        (should (equal (current-kill 0) "alice"))))))

(ert-deftest clutch-test-copy-format-commands-copy-visible-content ()
  "Public CSV and TSV copy commands should copy through the real entry point."
  (dolist (case '((clutch-result-copy-csv "name\nalice")
                  (clutch-result-copy-org-table "| name |\n|---|\n| alice |")
                  (clutch-result-copy-tsv "alice")))
    (pcase-let ((`(,command ,expected-text) case))
      (ert-info ((symbol-name command))
        (with-temp-buffer
          (let (kill-ring kill-ring-yank-pointer)
            (setq-local clutch--result-columns '("id" "name")
                        clutch--result-rows '((1 "alice")))
            (cl-letf (((symbol-function 'transient-args)
                       (lambda (_prefix) nil))
                      ((symbol-function 'transient-arg-value)
                       (lambda (_flag _args) nil))
                      ((symbol-function 'use-region-p)
                       (lambda () nil))
                      ((symbol-function 'clutch--cell-at-point)
                       (lambda () '(0 1 "alice"))))
              (funcall command)
              (should (equal (current-kill 0) expected-text)))))))))

(ert-deftest clutch-test-copy-fmt-with-refine-uses-refined-rectangle ()
  "Refined copy should copy the final rectangle, not the initial region."
  (with-temp-buffer
    (let (kill-ring kill-ring-yank-pointer)
      (setq-local clutch--result-columns '("id" "name" "score")
                  clutch--result-rows '((1 "alice" 10)
                                        (2 "bob" 20)
                                        (3 "cam" 30)))
      (cl-letf (((symbol-function 'use-region-p) (lambda () t))
                ((symbol-function 'transient-args)
                 (lambda (_prefix) '("--refine")))
                ((symbol-function 'transient-arg-value)
                 (lambda (flag args)
                   (and (equal flag "--refine")
                        (member "--refine" args))))
                ((symbol-function 'clutch-result--region-rectangle-indices)
                 (lambda () '((0 1 2) . (1 2))))
                ((symbol-function 'clutch-result--start-refine)
                 (lambda (_rect callback)
                   (funcall callback '((0 2) . (2))))))
        (clutch-result--copy-fmt 'csv)
        (should (equal (current-kill 0) "score\n10\n30"))))))

(ert-deftest clutch-test-copy-org-table-escapes-table-sensitive-content ()
  "Org table copy should keep one logical table row per result row."
  (with-temp-buffer
    (setq-local clutch--result-columns '("name" "note")
                clutch--result-rows '(("alice" "a|b")
                                      ("bob" "x\ny")))
    (let (kill-ring kill-ring-yank-pointer)
      (cl-letf (((symbol-function 'use-region-p) (lambda () t))
                ((symbol-function 'clutch-result--region-rectangle-indices)
                 (lambda () '((0 1) 0 1))))
        (clutch-result-copy 'org-table)
        (should (equal (current-kill 0)
                       "| name | note |\n|---+---|\n| alice | a\\vertb |\n| bob | x\\ny |"))))))

(ert-deftest clutch-test-copy-csv-via-unified-entry-uses-region-rectangle ()
  "Unified CSV copy should use rectangle row/column bounds when region is active."
  (with-temp-buffer
    (let (kill-ring kill-ring-yank-pointer)
      (setq-local clutch--result-columns '("c0" "c1" "c2"))
      (setq-local clutch--result-rows '((a0 a1 a2) (b0 b1 b2) (c0 c1 c2)))
        (cl-letf (((symbol-function 'use-region-p) (lambda () t))
                ((symbol-function 'clutch-result--region-rectangle-indices)
                 (lambda () '((0 1) 1 2))))
        (clutch-result-copy 'csv)
        (should (equal (current-kill 0) "c1,c2\na1,a2\nb1,b2"))))))

(ert-deftest clutch-test-copy-csv-without-region-copies-current-cell ()
  "Unified CSV copy should use current cell when region is inactive."
  (with-temp-buffer
    (setq-local clutch--result-columns '("c0" "c1"))
    (setq-local clutch--result-rows '((a0 a1)))
    (let (kill-ring kill-ring-yank-pointer)
      (cl-letf (((symbol-function 'use-region-p) (lambda () nil))
                ((symbol-function 'clutch--cell-at-point)
                 (lambda () '(0 1 a1))))
        (clutch-result-copy 'csv)
        (should (equal (current-kill 0) "c1\na1"))))))

(ert-deftest clutch-test-copy-insert-via-unified-entry-uses-region-rectangle ()
  "Unified INSERT copy should use rectangle row/column bounds when region is active."
  (with-temp-buffer
    (let (kill-ring kill-ring-yank-pointer)
      (setq-local clutch-connection 'fake-conn)
      (setq-local clutch--result-columns '("id" "name" "age"))
      (setq-local clutch--result-rows '((1 "a" 10) (2 "b" 20))
                  clutch--last-query "SELECT id, name, age FROM t")
      (cl-letf (((symbol-function 'use-region-p) (lambda () t))
                ((symbol-function 'clutch-result--region-rectangle-indices)
                 (lambda () '((0 1) 0 1)))
                ((symbol-function 'clutch-db-escape-identifier)
                 (lambda (_conn s) (format "\"%s\"" s)))
                ((symbol-function 'clutch-db-value-to-literal)
                 (lambda (_conn v &optional _formatter) (format "'%s'" v))))
        (clutch-result-copy 'insert)
        (should (string-match-p "INSERT INTO \"t\" (\"id\", \"name\") VALUES ('1', 'a');"
                                (current-kill 0)))
        (should (string-match-p "INSERT INTO \"t\" (\"id\", \"name\") VALUES ('2', 'b');"
                                (current-kill 0)))))))

(ert-deftest clutch-test-copy-insert-without-region-copies-current-cell ()
  "Unified INSERT copy should use current cell when region is inactive."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn)
    (setq-local clutch--result-columns '("id" "name"))
    (setq-local clutch--result-rows '((1 "a"))
                clutch--last-query "SELECT id, name FROM t")
    (let (kill-ring kill-ring-yank-pointer)
      (cl-letf (((symbol-function 'use-region-p) (lambda () nil))
                ((symbol-function 'clutch--cell-at-point)
                 (lambda () '(0 1 "a")))
                ((symbol-function 'clutch-db-escape-identifier)
                 (lambda (_conn s) (format "\"%s\"" s)))
                ((symbol-function 'clutch-db-value-to-literal)
                 (lambda (_conn v &optional _formatter) (format "'%s'" v))))
        (clutch-result-copy 'insert)
        (should (equal (current-kill 0)
                       "INSERT INTO \"t\" (\"name\") VALUES ('a');"))))))

;;;; Refine

(defun clutch-test--setup-refine-result-buffer ()
  "Populate the current buffer with a small rendered result table."
  (clutch-result-mode)
  (setq-local clutch--result-columns '("id" "name")
              clutch--result-column-defs '((:name "id" :type-category numeric)
                                           (:name "name" :type-category text))
              clutch--result-rows '((1 "alice") (2 "bob") (3 "carol"))
              clutch--filtered-rows nil
              clutch--pending-edits nil
              clutch--pending-deletes nil
              clutch--pending-inserts nil
              clutch--marked-rows nil
              clutch--sort-column nil
              clutch--sort-descending nil
              clutch--page-current 0
              clutch--page-total-rows 3
              clutch--column-widths [2 5])
  (clutch--refresh-display))

(ert-deftest clutch-test-refine-start-activates-mode-and-overlays ()
  "Starting refine mode should enable the mode and create selection overlays."
  (with-temp-buffer
    (clutch-test--setup-refine-result-buffer)
    (let ((callback (lambda (_rect) nil)))
      (clutch-result--start-refine '((0 1 2) . (0 1)) callback)
      (should clutch-refine-mode)
      (should (equal clutch--refine-rect '((0 1 2) . (0 1))))
      (should clutch--refine-overlays)
      (should (eq clutch--refine-callback callback)))))

(ert-deftest clutch-test-refine-toggle-row-excludes-and-includes ()
  "Refine mode should toggle row exclusion at point."
  (with-temp-buffer
    (clutch-test--setup-refine-result-buffer)
    (clutch-result--start-refine '((0 1 2) . (0 1)) #'ignore)
    (goto-char (point-min))
    (let ((match (text-property-search-forward 'clutch-row-idx 1 #'eq)))
      (should match)
      (goto-char (prop-match-beginning match)))
    (clutch-refine-toggle-row)
    (should (equal clutch--refine-excluded-rows '(1)))
    (clutch-refine-toggle-row)
    (should-not clutch--refine-excluded-rows)))

(ert-deftest clutch-test-refine-toggle-col-excludes-and-includes ()
  "Refine mode should toggle column exclusion at point."
  (with-temp-buffer
    (clutch-test--setup-refine-result-buffer)
    (clutch-result--start-refine '((0 1 2) . (0 1)) #'ignore)
    (goto-char (point-min))
    (let ((match (text-property-search-forward 'clutch-col-idx 1 #'eq)))
      (should match)
      (goto-char (prop-match-beginning match)))
    (clutch-refine-toggle-col)
    (should (equal clutch--refine-excluded-cols '(1)))
    (clutch-refine-toggle-col)
    (should-not clutch--refine-excluded-cols)))

(ert-deftest clutch-test-refine-confirm-calls-callback-with-filtered-rect ()
  "Refine confirm should pass the remaining rectangle to the callback."
  (with-temp-buffer
    (clutch-test--setup-refine-result-buffer)
    (let (seen)
      (clutch-result--start-refine '((0 1 2) . (0 1))
                                   (lambda (rect) (setq seen rect)))
      (goto-char (point-min))
      (let ((row-match (text-property-search-forward 'clutch-row-idx 1 #'eq)))
        (should row-match)
        (goto-char (prop-match-beginning row-match)))
      (clutch-refine-toggle-row)
      (goto-char (point-min))
      (let ((col-match (text-property-search-forward 'clutch-col-idx 0 #'eq)))
        (should col-match)
        (goto-char (prop-match-beginning col-match)))
      (clutch-refine-toggle-col)
      (clutch-refine-confirm)
      (should (equal seen '((0 2) . (1))))
      (should-not clutch-refine-mode))))

(ert-deftest clutch-test-refine-confirm-errors-when-all-rows-excluded ()
  "Refine confirm should error when no rows remain."
  (with-temp-buffer
    (clutch-test--setup-refine-result-buffer)
    (clutch-result--start-refine '((0) . (0 1)) #'ignore)
    (goto-char (point-min))
    (let ((match (text-property-search-forward 'clutch-row-idx 0 #'eq)))
      (should match)
      (goto-char (prop-match-beginning match)))
    (clutch-refine-toggle-row)
    (should-error (clutch-refine-confirm) :type 'user-error)))

(ert-deftest clutch-test-refine-confirm-errors-when-all-cols-excluded ()
  "Refine confirm should error when no columns remain."
  (with-temp-buffer
    (clutch-test--setup-refine-result-buffer)
    (clutch-result--start-refine '((0 1) . (0)) #'ignore)
    (goto-char (point-min))
    (let ((match (text-property-search-forward 'clutch-col-idx 0 #'eq)))
      (should match)
      (goto-char (prop-match-beginning match)))
    (clutch-refine-toggle-col)
    (should-error (clutch-refine-confirm) :type 'user-error)))

(ert-deftest clutch-test-refine-cancel-does-not-call-callback ()
  "Refine cancel should exit without invoking the callback."
  (with-temp-buffer
    (clutch-test--setup-refine-result-buffer)
    (let ((called nil))
      (clutch-result--start-refine '((0 1 2) . (0 1))
                                   (lambda (_rect) (setq called t)))
      (clutch-refine-cancel)
      (should-not called)
      (should-not clutch-refine-mode))))

;;;; Page navigation and sorting

(ert-deftest clutch-test-page-navigation-boundary-errors ()
  "Page commands should error at their known boundaries."
  (dolist (case '((next clutch-result-next-page)
                  (previous clutch-result-prev-page)
                  (first clutch-result-first-page)))
    (pcase-let ((`(,label ,command) case))
      (ert-info ((format "case: %s" label))
        (with-temp-buffer
          (pcase label
            ('next
             (setq-local clutch--result-rows (make-list 10 '(1))
                         clutch--page-has-more nil
                         clutch-result-max-rows 10))
            ((or 'previous 'first)
             (setq-local clutch--page-current 0)))
          (should-error (funcall command) :type 'user-error))))))

(ert-deftest clutch-test-page-navigation-executes-target-pages ()
  "Page commands should dispatch to the expected target page."
  (dolist (case '((next clutch-result-next-page 0 1)
                  (previous clutch-result-prev-page 3 2)
                  (first clutch-result-first-page 5 0)))
    (pcase-let ((`(,label ,command ,current-page ,expected-page) case))
      (ert-info ((format "case: %s" label))
        (with-temp-buffer
          (setq-local clutch--result-rows (make-list 50 '(1))
                      clutch-result-max-rows 50
                      clutch--page-current current-page
                      clutch--page-has-more t)
          (let (executed-page)
            (cl-letf (((symbol-function 'clutch-result--execute-page)
                       (lambda (page) (setq executed-page page))))
              (funcall command)
              (should (= executed-page expected-page)))))))))

(ert-deftest clutch-test-last-page-calculates-correct-page ()
  "Last page should use a last-window offset from total rows and page size."
  (with-temp-buffer
    (setq-local clutch--page-current 0
                clutch--page-total-rows 237
                clutch-result-max-rows 50)
    (let (executed-page executed-offset)
      (cl-letf (((symbol-function 'clutch-result--execute-page-at-offset)
                 (lambda (offset page)
                   (setq executed-offset offset
                         executed-page page))))
        (clutch-result-last-page)
        (should (= executed-page 4))
        (should (= executed-offset 187))))))

(ert-deftest clutch-test-last-page-errors-when-already-last ()
  "Last page should error when already on the last page."
  (with-temp-buffer
    (setq-local clutch--page-current 4
                clutch--page-offset 187
                clutch--page-total-rows 237
                clutch-result-max-rows 50)
    (should-error (clutch-result-last-page) :type 'user-error)))

(ert-deftest clutch-test-last-page-shifts-normal-last-page-to-last-window ()
  "M-> should show the final window even when already on the last page index."
  (with-temp-buffer
    (setq-local clutch--page-current 1
                clutch--page-offset 500
                clutch--page-total-rows 578
                clutch-result-max-rows 500)
    (let (executed-page executed-offset)
      (cl-letf (((symbol-function 'clutch-result--execute-page-at-offset)
                 (lambda (offset page)
                   (setq executed-offset offset
                         executed-page page))))
        (clutch-result-last-page)
        (should (= executed-page 1))
        (should (= executed-offset 78))))))

(ert-deftest clutch-test-last-page-single-page-result ()
  "Last page should error when total rows fit in one page."
  (with-temp-buffer
    (setq-local clutch--page-current 0
                clutch--page-total-rows 30
                clutch-result-max-rows 50)
    (should-error (clutch-result-last-page) :type 'user-error)))

(ert-deftest clutch-test-sort-by-column-toggles-direction ()
  "Sorting the same column twice should toggle ascending/descending."
  (with-temp-buffer
    (setq-local clutch--sort-column "name"
                clutch--sort-descending nil)
    (let (sort-args)
      (cl-letf (((symbol-function 'clutch-result--read-column)
                 (lambda () "name"))
                ((symbol-function 'clutch-result--sort)
                 (lambda (col desc) (setq sort-args (list col desc)))))
        (clutch-result-sort-by-column)
        (should (equal sort-args '("name" t)))))))

(ert-deftest clutch-test-sort-by-column-new-column-defaults-ascending ()
  "Sorting a different column should default to ascending."
  (with-temp-buffer
    (setq-local clutch--sort-column "name"
                clutch--sort-descending t)
    (let (sort-args)
      (cl-letf (((symbol-function 'clutch-result--read-column)
                 (lambda () "age"))
                ((symbol-function 'clutch-result--sort)
                 (lambda (col desc) (setq sort-args (list col desc)))))
        (clutch-result-sort-by-column)
        (should (equal sort-args '("age" nil)))))))

(ert-deftest clutch-test-sort-by-column-desc-always-descending ()
  "Sort descending command should always pass descending=t."
  (with-temp-buffer
    (let (sort-args)
      (cl-letf (((symbol-function 'clutch-result--read-column)
                 (lambda () "created_at"))
                ((symbol-function 'clutch-result--sort)
                 (lambda (col desc) (setq sort-args (list col desc)))))
        (clutch-result-sort-by-column-desc)
        (should (equal sort-args '("created_at" t)))))))

(ert-deftest clutch-test-sort-rejects-hidden-row-identity-column ()
  "Server-side sort should only accept visible user columns."
  (with-temp-buffer
    (setq-local clutch--result-server-rewritable t
                clutch--result-columns '("clutch__rid_0" "id" "name")
                clutch--result-column-defs
                '((:name "clutch__rid_0" :hidden t)
                  (:name "id")
                  (:name "name")))
    (let (paged)
      (cl-letf (((symbol-function 'clutch-result--execute-page)
                 (lambda (&rest _args) (setq paged t))))
        (let ((err (should-error (clutch-result--sort "clutch__rid_0" nil)
                                 :type 'user-error)))
          (should (string-match-p "Column clutch__rid_0 not found"
                                  (error-message-string err))))
        (should-not paged)))))

(ert-deftest clutch-test-sort-errors-for-nonrewritable-query-result ()
  "Server-side sorting should not rewrite arbitrary query results."
  (with-temp-buffer
    (setq-local clutch--result-server-rewritable nil
                clutch--result-columns '("id" "name" "id"))
    (let (paged)
      (cl-letf (((symbol-function 'clutch-result--execute-page)
                 (lambda (&rest _args) (setq paged t))))
        (let ((err (should-error (clutch-result--sort "id" nil)
                                 :type 'user-error)))
          (should (string-match-p "Server-side sort"
                                  (error-message-string err))))
        (should-not paged)))))

;;;; Connection — build, timeout, and lifecycle

(ert-deftest clutch-test-backend-key-from-conn-returns-nil-for-non-jdbc-or-opaque-connections ()
  "Backend key detection should tolerate non-JDBC and opaque connections."
  (dolist (case '(non-jdbc opaque))
    (ert-info ((format "case: %s" case))
      (cl-letf (((symbol-function 'clutch-db-display-name)
                 (lambda (_conn)
                   (pcase case
                     ('non-jdbc "DuckDB")
                     ('opaque
                      (signal 'cl-no-applicable-method
                              '(clutch-db-display-name fake-conn)))))))
        (should-not (clutch--backend-key-from-conn 'fake-conn))))))

(ert-deftest clutch-test-backend-key-from-conn-swallows-jdbc-param-errors ()
  "Backend key detection should return nil when JDBC plist access fails.
The function catches `clutch-db-error' and `wrong-type-argument' to avoid
crashing the UI layer."
  (let* ((sentinel (list :driver 'oracle))
         (conn (make-clutch-jdbc-conn :params sentinel)))
    (cl-letf (((symbol-function 'plist-get)
               (lambda (plist prop &optional predicate)
                 (if (eq plist sentinel)
                     (signal 'clutch-db-error '("backend key boom"))
                   (let ((tail plist)
                         found)
                     (while tail
                       (when (funcall (or predicate #'eq) (car tail) prop)
                         (setq found (cadr tail)
                               tail nil))
                       (setq tail (cddr tail)))
                     found))))
              ((symbol-function 'clutch-db-display-name)
               (lambda (_conn) nil)))
      (should-not (clutch--backend-key-from-conn conn)))))

(ert-deftest clutch-test-backend-key-from-params-prefers-concrete-backend ()
  "Concrete :backend should win over generic JDBC :driver metadata."
  (should (eq (clutch--backend-key-from-params
               '(:backend oracle :driver jdbc))
              'oracle))
  (should (eq (clutch--backend-key-from-params
               '(:backend jdbc :driver oracle))
              'oracle))
  (should (eq (clutch--backend-key-from-params
               '(:backend jdbc :driver jdbc :display-name "KingbaseES"))
              'jdbc)))

(ert-deftest clutch-test-connection-key ()
  "Test connection key generation."
  (require 'clutch-db-mysql)
  (require 'mysql)
  (let ((conn (make-mysql-conn :host "localhost" :port 3306
                                    :user "root" :database "test")))
    (let ((key (clutch--connection-key conn)))
      (should (stringp key))
      (should (string-match-p "localhost" key))
      (should (string-match-p "3306" key))
      (should (string-match-p "root" key))
      (should (string-match-p "test" key)))))

(ert-deftest clutch-test-connection-key-omits-empty-user-prefix ()
  "Connection keys should not show ?@ when a backend has no user."
  (cl-letf (((symbol-function 'clutch--backend-key-from-conn)
             (lambda (_conn) 'mongodb))
            ((symbol-function 'clutch-db-user) (lambda (_conn) nil))
            ((symbol-function 'clutch-db-host) (lambda (_conn) "127.0.0.1"))
            ((symbol-function 'clutch-db-port) (lambda (_conn) 27017))
            ((symbol-function 'clutch-db-database) (lambda (_conn) "app")))
    (should (equal (clutch--connection-key 'fake-conn)
                   "127.0.0.1:27017/app"))))

(ert-deftest clutch-test-connection-display-key-prefers-remote-ssh-endpoint ()
  "Connection labels should keep the remote endpoint visible when SSH is used."
  (let ((clutch--connection-remote-params-cache (make-hash-table :test 'eq))
        (conn 'fake-conn))
    (puthash conn '(:host "db.internal" :port 5432 :ssh-host "bastion-prod")
             clutch--connection-remote-params-cache)
    (cl-letf (((symbol-function 'clutch-db-user) (lambda (_conn) "alice"))
              ((symbol-function 'clutch-db-host) (lambda (_conn) "127.0.0.1"))
              ((symbol-function 'clutch-db-port) (lambda (_conn) 40123))
              ((symbol-function 'clutch-db-database) (lambda (_conn) "appdb"))
              ((symbol-function 'clutch-db-display-name) (lambda (_conn) "PostgreSQL")))
      (should (equal (clutch--connection-key conn)
                     "alice@db.internal:5432/appdb"))
      (should (equal (clutch--connection-display-key conn)
                     "alice@db.internal via bastion-prod")))))

(ert-deftest clutch-test-connection-display-key-prefers-remote-tramp-endpoint ()
  "Connection labels should keep the remote endpoint visible when TRAMP is used."
  (let ((clutch--connection-remote-params-cache (make-hash-table :test 'eq))
        (conn 'fake-conn))
    (puthash conn '(:host "db" :port 5432
                    :tramp-default-directory "/ssh:devbox:/workspace/")
             clutch--connection-remote-params-cache)
    (cl-letf (((symbol-function 'clutch-db-user) (lambda (_conn) "alice"))
              ((symbol-function 'clutch-db-host) (lambda (_conn) "127.0.0.1"))
              ((symbol-function 'clutch-db-port) (lambda (_conn) 40123))
              ((symbol-function 'clutch-db-database) (lambda (_conn) "appdb"))
              ((symbol-function 'clutch-db-display-name) (lambda (_conn) "PostgreSQL")))
      (should (equal (clutch--connection-key conn)
                     "alice@db:5432/appdb"))
      (should (equal (clutch--connection-display-key conn)
                     "alice@db via devbox")))))

(ert-deftest clutch-test-prepare-origin-auto-adds-source-tramp-context ()
  "TRAMP source context should be copied into network params under auto policy."
  (let ((clutch-tramp-context-policy 'auto))
    (should (equal
             (clutch--prepare-connection-origin-params
              '(:backend pg :host "db" :port 5432 :user "app")
              "/ssh:devbox:/workspace/")
             '(:backend pg :host "db" :port 5432 :user "app"
               :tramp-default-directory "/ssh:devbox:/workspace/")))))

(ert-deftest clutch-test-prepare-origin-auto-adds-source-container-tramp-context ()
  "Container TRAMP source context should be copied into network params."
  (let ((clutch-tramp-context-policy 'auto))
    (should (equal
             (clutch--prepare-connection-origin-params
              '(:backend pg :host "db" :port 5432 :user "app")
              "/docker:vscode@app:/workspace/")
             '(:backend pg :host "db" :port 5432 :user "app"
               :tramp-default-directory "/docker:vscode@app:/workspace/")))))

(ert-deftest clutch-test-prepare-connection-params-normalizes-tramp-alias ()
  "The shorter :tramp key should be canonicalized before connection setup."
  (let ((params (clutch-prepare-connection-params
                 '(:backend pg :host "db" :port 5432 :user "app"
                   :tramp "/ssh:devbox:/workspace/"))))
    (should-not (plist-member params :tramp))
    (should (equal (plist-get params :tramp-default-directory)
                   "/ssh:devbox:/workspace/"))))

(ert-deftest clutch-test-prepare-connection-params-rejects-conflicting-tramp-aliases ()
  "The short and long TRAMP keys must not describe different origins."
  (should-error
   (clutch-prepare-connection-params
    '(:backend pg :host "db" :port 5432 :user "app"
      :tramp "/ssh:a:/work/"
      :tramp-default-directory "/ssh:b:/work/"))
   :type 'user-error))

(ert-deftest clutch-test-prepare-origin-ask-can-decline-source-tramp-context ()
  "Declining the TRAMP prompt should leave network params local."
  (let ((clutch-tramp-context-policy 'ask)
        asked)
    (cl-letf (((symbol-function 'y-or-n-p)
               (lambda (_prompt)
                 (setq asked t)
                 nil)))
      (should (equal
               (clutch--prepare-connection-origin-params
                '(:backend pg :host "db" :port 5432 :user "app")
                "/ssh:devbox:/workspace/")
               '(:backend pg :host "db" :port 5432 :user "app")))
      (should asked))))

(ert-deftest clutch-test-prepare-origin-does-not-infer-over-ssh-transport ()
  "Existing SSH forward configuration must win over current TRAMP context."
  (let ((clutch-tramp-context-policy 'auto)
        (params '(:backend pg :host "db" :port 5432 :user "app"
                  :ssh-host "bastion-prod")))
    (should (equal
             (clutch--prepare-connection-origin-params
              params "/ssh:devbox:/workspace/")
             params))))

(ert-deftest clutch-test-prepare-origin-skips-unforwardable-params ()
  "TRAMP source inference should not attach to SQLite or raw URL profiles."
  (let ((clutch-tramp-context-policy 'auto))
    (should-not
     (plist-member
      (clutch--prepare-connection-origin-params
       '(:backend sqlite :database "/tmp/app.db")
       "/ssh:devbox:/workspace/")
      :tramp-default-directory))
    (should-not
     (plist-member
      (clutch--prepare-connection-origin-params
       '(:backend oracle :url "jdbc:oracle:thin:@//db:1521/ORCL")
       "/ssh:devbox:/workspace/")
      :tramp-default-directory))))

(ert-deftest clutch-test-build-conn-leaves-timeout-defaults-to-backends ()
  "Connection building should not synthesize backend timeout defaults."
  (let (captured)
    (cl-letf (((symbol-function 'clutch--resolve-password)
               (lambda (_params) nil))
              ((symbol-function 'clutch-db-connect)
               (lambda (_backend params)
                 (setq captured params)
                 'fake-conn)))
      (dolist (params '((:backend mysql :host "127.0.0.1" :port 3306 :user "u")
                        (:backend pg :host "127.0.0.1" :port 5432 :user "u")
                        (:backend oracle :host "db" :port 1521 :user "u")
                        (:backend sqlite :database ":memory:")))
        (setq captured nil)
        (clutch--build-conn params)
        (should-not (plist-member captured :connect-timeout))
        (should-not (plist-member captured :read-idle-timeout))
        (should-not (plist-member captured :query-timeout))
        (should-not (plist-member captured :rpc-timeout))))))

(ert-deftest clutch-test-build-conn-rewrites-network-endpoint-through-ssh-tunnel ()
  "SSH-backed connections should target the local forwarded port."
  (let ((clutch--connection-remote-params-cache (make-hash-table :test 'eq))
        (clutch--connection-transport-cache (make-hash-table :test 'eq))
        captured)
    (cl-letf (((symbol-function 'clutch--resolve-password)
               (lambda (_params) nil))
              ((symbol-function 'clutch--start-ssh-tunnel)
               (lambda (params)
                 (should (equal (plist-get params :ssh-host) "bastion-prod"))
                 '(:process fake-proc :local-port 40123 :ssh-host "bastion-prod")))
              ((symbol-function 'clutch-db-connect)
               (lambda (_backend params)
                 (setq captured params)
                 'fake-conn)))
      (should (eq (clutch--build-conn
                   '(:backend pg
                     :host "db.internal"
                     :port 5432
                     :user "alice"
                     :database "appdb"
                     :ssh-host "bastion-prod"))
                  'fake-conn))
      (should (equal (plist-get captured :host) "127.0.0.1"))
      (should (= (plist-get captured :port) 40123))
      (should (equal (plist-get (gethash 'fake-conn clutch--connection-remote-params-cache)
                                :host)
                     "db.internal"))
      (should (equal (plist-get (gethash 'fake-conn clutch--connection-transport-cache)
                                :ssh-host)
                     "bastion-prod")))))

(ert-deftest clutch-test-build-conn-direct-first-selects-direct-or-ssh ()
  "Direct-first SSH mode should probe direct TCP before tunneling."
  (dolist (direct-open '(t nil))
    (ert-info ((format "direct-open=%S" direct-open))
      (let ((clutch--connection-remote-params-cache (make-hash-table :test 'eq))
            (clutch--connection-transport-cache (make-hash-table :test 'eq))
            captured probed tunnel-params attempts restored)
        (cl-letf (((symbol-function 'clutch--resolve-password)
                   (lambda (_params) nil))
                  ((symbol-function 'clutch--tcp-endpoint-open-p)
                   (lambda (host port timeout)
                     (setq probed (list host port timeout))
                     direct-open))
                  ((symbol-function 'clutch--start-ssh-tunnel)
                   (lambda (params)
                     (setq tunnel-params params)
                     '(:process fake-proc :local-port 40123
                       :ssh-host "bastion-prod")))
                  ((symbol-function 'clutch-db-connect)
                   (lambda (_backend params)
                     (setq attempts (append attempts (list params)))
                     (setq captured params)
                     'fake-conn))
                  ((symbol-function 'clutch-db--restore-connection-timeouts)
                   (lambda (conn params)
                     (setq restored (list conn params)))))
          (should (eq (clutch--build-conn
                       '(:backend pg
                         :host "db.internal"
                         :port 5432
                         :user "alice"
                         :database "appdb"
                         :ssh-host "bastion-prod"
                         :ssh-tunnel direct-first))
                      'fake-conn))
          (should (equal probed
                         (list "db.internal" 5432
                               clutch--ssh-direct-first-probe-timeout)))
          (should-not (plist-member captured :ssh-tunnel))
          (should (= (length attempts) 1))
          (if direct-open
              (progn
                (should (equal (plist-get captured :host) "db.internal"))
                (should (= (plist-get captured :port) 5432))
                (should (= (plist-get captured :connect-timeout)
                           clutch--ssh-direct-first-connect-timeout))
                (should (= (plist-get captured :read-idle-timeout)
                           clutch--ssh-direct-first-connect-timeout))
                (should-not (plist-member captured :rpc-timeout))
                (should (equal (car restored) 'fake-conn))
                (should (equal (plist-get (cadr restored) :host)
                               "db.internal"))
                (should-not tunnel-params)
                (should-not (gethash 'fake-conn
                                     clutch--connection-transport-cache)))
            (should (equal (plist-get tunnel-params :ssh-host) "bastion-prod"))
            (should (equal (plist-get captured :host) "127.0.0.1"))
            (should (= (plist-get captured :port) 40123))
            (should-not (plist-member captured :connect-timeout))
            (should-not (plist-member captured :read-idle-timeout))
            (should-not restored)
            (should (equal (plist-get
                            (gethash 'fake-conn
                                     clutch--connection-transport-cache)
                            :ssh-host)
                           "bastion-prod"))))))))

(ert-deftest clutch-test-build-conn-direct-first-falls-back-after-db-error ()
  "Direct-first should require a successful direct database connection."
  (let ((clutch--connection-remote-params-cache (make-hash-table :test 'eq))
        (clutch--connection-transport-cache (make-hash-table :test 'eq))
        attempts tunnel-params restored)
    (cl-letf (((symbol-function 'clutch--resolve-password)
               (lambda (_params) nil))
              ((symbol-function 'clutch--tcp-endpoint-open-p)
               (lambda (_host _port _timeout) t))
              ((symbol-function 'clutch--start-ssh-tunnel)
               (lambda (params)
                 (setq tunnel-params params)
                 '(:process fake-proc :local-port 40123
                   :ssh-host "bastion-prod")))
              ((symbol-function 'clutch-db-connect)
               (lambda (_backend params)
                 (setq attempts (append attempts (list params)))
                 (if (equal (plist-get params :host) "db.internal")
                     (signal 'clutch-db-error
                             '("Connection closed by server"))
                   'fake-conn)))
              ((symbol-function 'clutch-db--restore-connection-timeouts)
               (lambda (conn params)
                 (setq restored (list conn params)))))
      (should (eq (clutch--build-conn
                   '(:backend oracle
                     :host "db.internal"
                     :port 1521
                     :user "alice"
                     :database "ORCL"
                     :ssh-host "bastion-prod"
                     :ssh-tunnel direct-first))
                  'fake-conn))
      (should (= (length attempts) 2))
      (should (equal (plist-get (nth 0 attempts) :host) "db.internal"))
      (should (= (plist-get (nth 0 attempts) :connect-timeout) 1))
      (should (= (plist-get (nth 0 attempts) :rpc-timeout) 2))
      (should-not (plist-member (nth 0 attempts) :read-idle-timeout))
      (should (equal (plist-get (nth 1 attempts) :host) "127.0.0.1"))
      (should (= (plist-get (nth 1 attempts) :port) 40123))
      (should-not (plist-member (nth 1 attempts) :connect-timeout))
      (should-not (plist-member (nth 1 attempts) :read-idle-timeout))
      (should (equal (plist-get tunnel-params :ssh-host) "bastion-prod"))
      (should-not restored)
      (should (equal (plist-get (gethash 'fake-conn
                                          clutch--connection-transport-cache)
                                :ssh-host)
                     "bastion-prod")))))

(ert-deftest clutch-test-db-mysql-restore-connection-timeouts ()
  "Restoring MySQL timeout state should undo provisional direct-connect limits."
  (require 'clutch-db-mysql)
  (require 'mysql)
  (let ((conn (make-mysql-conn :read-idle-timeout 1)))
    (clutch-db--restore-connection-timeouts
     conn '(:host "db.internal" :user "alice" :read-idle-timeout 42))
    (should (= (mysql-conn-read-idle-timeout conn) 42))))

(ert-deftest clutch-test-build-conn-rewrites-network-endpoint-through-tramp-forward ()
  "TRAMP-backed connections should target the local forwarded port."
  (let ((clutch--connection-remote-params-cache (make-hash-table :test 'eq))
        (clutch--connection-transport-cache (make-hash-table :test 'eq))
        captured)
    (cl-letf (((symbol-function 'clutch--resolve-password)
               (lambda (_params) nil))
              ((symbol-function 'clutch--start-tramp-tcp-forward)
               (lambda (params)
                 (should (equal (plist-get params :tramp-default-directory)
                                "/ssh:devbox:/workspace/"))
                 '(:kind tramp
                   :process fake-listener
                   :local-port 40124
                   :tramp-default-directory "/ssh:devbox:/workspace/")))
              ((symbol-function 'clutch-db-connect)
               (lambda (_backend params)
                 (setq captured params)
                 'fake-conn)))
      (should (eq (clutch--build-conn
                   '(:backend pg
                     :host "db"
                     :port 5432
                     :user "alice"
                     :database "appdb"
                     :tramp-default-directory "/ssh:devbox:/workspace/"))
                  'fake-conn))
      (should (equal (plist-get captured :host) "127.0.0.1"))
      (should (= (plist-get captured :port) 40124))
      (should-not (plist-member captured :tramp-default-directory))
      (should (equal (plist-get (gethash 'fake-conn clutch--connection-remote-params-cache)
                                :host)
                     "db"))
      (should (equal (plist-get (gethash 'fake-conn clutch--connection-remote-params-cache)
                                :tramp-default-directory)
                     "/ssh:devbox:/workspace/"))
      (should (eq (plist-get (gethash 'fake-conn clutch--connection-transport-cache)
                             :kind)
                  'tramp)))))

(ert-deftest clutch-test-open-connection-supports-tramp-alias ()
  "The public connection API should support the short :tramp key."
  (let ((clutch--connection-remote-params-cache (make-hash-table :test 'eq))
        (clutch--connection-transport-cache (make-hash-table :test 'eq))
        captured)
    (cl-letf (((symbol-function 'clutch--resolve-password)
               (lambda (_params) nil))
              ((symbol-function 'clutch--start-tramp-tcp-forward)
               (lambda (params)
                 (should-not (plist-member params :tramp))
                 (should (equal (plist-get params :tramp-default-directory)
                                "/ssh:devbox:/workspace/"))
                 '(:kind tramp
                   :process fake-listener
                   :local-port 40124
                   :tramp-default-directory "/ssh:devbox:/workspace/")))
              ((symbol-function 'clutch-db-connect)
               (lambda (_backend params)
                 (setq captured params)
                 'fake-conn)))
      (should (eq (clutch-open-connection
                   '(:backend pg
                     :host "db"
                     :port 5432
                     :user "alice"
                     :database "appdb"
                     :tramp "/ssh:devbox:/workspace/"))
                  'fake-conn))
      (should (equal (plist-get captured :host) "127.0.0.1"))
      (should (= (plist-get captured :port) 40124))
      (should-not (plist-member captured :tramp))
      (should-not (plist-member captured :tramp-default-directory)))))

(ert-deftest clutch-test-prepare-connect-params-rejects-ambiguous-transports ()
  "A connection must not combine SSH and TRAMP transports."
  (should-error
   (clutch--prepare-connect-params
    '(:backend pg
      :host "db"
      :port 5432
      :ssh-host "bastion-prod"
      :tramp "/ssh:devbox:/workspace/"))
   :type 'user-error))

(ert-deftest clutch-test-prepare-connect-params-validates-ssh-tunnel-mode ()
  "SSH tunnel mode should be explicit and tied to :ssh-host."
  (should-error
   (clutch--prepare-connect-params
    '(:backend pg :host "db" :port 5432 :ssh-tunnel direct-first))
   :type 'user-error)
  (should-error
   (clutch--prepare-connect-params
    '(:backend pg :host "db" :port 5432 :ssh-host "bastion-prod"
      :ssh-tunnel sometimes))
   :type 'user-error))

(ert-deftest clutch-test-build-conn-stops-ssh-tunnel-when-db-connect-fails ()
  "Failed DB connect should tear down the SSH tunnel it just opened."
  (let (stopped)
    (cl-letf (((symbol-function 'clutch--resolve-password)
               (lambda (_params) nil))
              ((symbol-function 'clutch--start-ssh-tunnel)
               (lambda (_params)
                 '(:process fake-proc :local-port 40123 :ssh-host "bastion-prod")))
              ((symbol-function 'process-live-p)
               (lambda (proc) (eq proc 'fake-proc)))
              ((symbol-function 'delete-process)
               (lambda (proc) (setq stopped proc)))
              ((symbol-function 'clutch-db-connect)
               (lambda (_backend _params)
                 (signal 'clutch-db-error '("db connect failed")))))
      (should-error
       (clutch--build-conn
        '(:backend pg
          :host "db.internal"
          :port 5432
          :user "alice"
          :database "appdb"
          :ssh-host "bastion-prod"))
       :type 'user-error)
      (should (eq stopped 'fake-proc)))))

(ert-deftest clutch-test-start-ssh-tunnel-rejects-jdbc-url ()
  "SSH tunneling should fail fast when the profile only provides a raw JDBC URL."
  (let (connect-called)
    (cl-letf (((symbol-function 'clutch--resolve-password)
               (lambda (_params) nil))
              ((symbol-function 'executable-find)
               (lambda (_name) "/usr/bin/ssh"))
              ((symbol-function 'clutch-db-connect)
               (lambda (&rest _args)
                 (setq connect-called t)
                 'unexpected)))
      (should-error
       (clutch--start-ssh-tunnel
        '(:backend oracle
          :url "jdbc:oracle:thin:@//db.example.com:1521/ORCL"
          :user "scott"
          :ssh-host "bastion-prod"))
       :type 'user-error)
      (should-not connect-called))))

(ert-deftest clutch-test-start-ssh-tunnel-starts-batch-tunnel-directly ()
  "SSH tunnel startup should rely on the real batch tunnel command."
  (let (process-file-called make-process-args)
    (cl-letf (((symbol-function 'executable-find)
               (lambda (_name) "/usr/bin/ssh"))
              ((symbol-function 'process-file)
               (lambda (&rest _args)
                 (setq process-file-called t)
                 0))
              ((symbol-function 'clutch--allocate-local-port)
               (lambda () 40123))
              ((symbol-function 'make-process)
                (lambda (&rest args)
                 (setq make-process-args args)
                 'fake-proc))
              ((symbol-function 'set-process-query-on-exit-flag) #'ignore)
              ((symbol-function 'clutch--wait-for-ssh-tunnel)
               (lambda (&rest _args) t)))
      (let ((tunnel (clutch--start-ssh-tunnel
                     '(:backend pg
                       :host "db.internal"
                       :port 5432
                       :ssh-host "bastion-prod"))))
        (should-not process-file-called)
        (should (eq (plist-get tunnel :process) 'fake-proc))
        (should (equal (plist-get make-process-args :command)
                       '("ssh"
                         "-N"
                         "-o" "BatchMode=yes"
                         "-o" "ExitOnForwardFailure=yes"
                         "-L" "127.0.0.1:40123:db.internal:5432"
                         "bastion-prod")))))))

(ert-deftest clutch-test-start-tramp-forward-rejects-jdbc-url ()
  "TRAMP forwarding should fail fast when the profile only provides a raw JDBC URL."
  (let (make-process-called)
    (cl-letf (((symbol-function 'make-process)
               (lambda (&rest _args)
                 (setq make-process-called t)
                 'unexpected)))
      (should-error
       (clutch--start-tramp-tcp-forward
        '(:backend oracle
          :url "jdbc:oracle:thin:@//db.example.com:1521/ORCL"
          :user "scott"
          :tramp-default-directory "/ssh:devbox:/workspace/"))
       :type 'user-error)
      (should-not make-process-called))))

(ert-deftest clutch-test-start-tramp-forward-starts-direct-ssh-forward ()
  "Direct ssh TRAMP forward startup should use OpenSSH -L."
  (let (make-process-args query-flag waited)
    (cl-letf (((symbol-function 'executable-find)
               (lambda (program)
                 (when (equal program "ssh") "/usr/bin/ssh")))
              ((symbol-function 'clutch--allocate-local-port)
               (lambda () 40124))
              ((symbol-function 'make-process)
               (lambda (&rest args)
                 (setq make-process-args args)
                 'fake-proc))
              ((symbol-function 'set-process-query-on-exit-flag)
               (lambda (proc flag)
                 (setq query-flag (list proc flag))))
              ((symbol-function 'clutch--wait-for-ssh-tunnel)
               (lambda (proc port params buffer timeout)
                 (setq waited (list proc port params buffer timeout)))))
      (let ((transport (clutch--start-tramp-tcp-forward
                        '(:backend pg
                          :host "db"
                          :port 5432
                          :tramp-default-directory "/ssh:devbox:/workspace/"))))
        (should (equal (plist-get make-process-args :command)
                       '("ssh"
                         "-N"
                         "-o" "BatchMode=yes"
                         "-o" "ExitOnForwardFailure=yes"
                         "-L" "127.0.0.1:40124:db:5432"
                         "devbox")))
        (should (equal query-flag '(fake-proc nil)))
        (should (eq (plist-get transport :kind) 'tramp))
        (should (eq (plist-get transport :process) 'fake-proc))
        (should (= (plist-get transport :local-port) 40124))
        (should (equal (plist-get transport :tramp-default-directory)
                       "/ssh:devbox:/workspace/"))
        (should (equal (nth 0 waited) 'fake-proc))
        (should (= (nth 1 waited) 40124))
        (should (equal (plist-get (nth 2 waited) :ssh-host) "devbox"))))))

(ert-deftest clutch-test-start-tramp-forward-starts-rpc-forward ()
  "tramp-rpc directories should be mapped to OpenSSH -L."
  (let ((old-bound (boundp 'tramp-rpc-use-controlmaster))
        (old-value (and (boundp 'tramp-rpc-use-controlmaster)
                        (symbol-value 'tramp-rpc-use-controlmaster)))
        make-process-args)
    (unwind-protect
        (progn
          (set 'tramp-rpc-use-controlmaster t)
          (cl-letf (((symbol-function 'executable-find)
                     (lambda (program)
                       (when (equal program "ssh") "/usr/bin/ssh")))
                    ((symbol-function 'clutch--allocate-local-port)
                     (lambda () 40124))
                    ((symbol-function 'make-process)
                     (lambda (&rest args)
                       (setq make-process-args args)
                       'fake-proc))
                    ((symbol-function 'set-process-query-on-exit-flag) #'ignore)
                    ((symbol-function 'clutch--wait-for-ssh-tunnel) #'ignore)
                    ((symbol-function 'tramp-rpc-controlmaster-options)
                     (lambda (vec)
                       (should (equal (tramp-file-name-method vec) "rpc"))
                       '("-o" "ControlMaster=auto"
                         "-o" "ControlPath=/tmp/tramp-rpc.sock"))))
            (clutch--start-tramp-tcp-forward
             '(:backend pg
               :host "127.0.0.1"
               :port 55433
               :tramp-default-directory "/rpc:devbox:/workspace/"))
            (should (equal (plist-get make-process-args :command)
                           '("ssh"
                             "-N"
                             "-o" "BatchMode=yes"
                             "-o" "ExitOnForwardFailure=yes"
                             "-L" "127.0.0.1:40124:127.0.0.1:55433"
                             "-o" "ControlMaster=auto"
                             "-o" "ControlPath=/tmp/tramp-rpc.sock"
                             "devbox")))))
      (if old-bound
          (set 'tramp-rpc-use-controlmaster old-value)
        (makunbound 'tramp-rpc-use-controlmaster)))))

(ert-deftest clutch-test-start-tramp-forward-maps-ssh-like-hops-to-proxyjump ()
  "SSH-like TRAMP hops should become an OpenSSH ProxyJump option."
  (let (make-process-args)
    (cl-letf (((symbol-function 'executable-find)
               (lambda (program)
                 (when (equal program "ssh") "/usr/bin/ssh")))
              ((symbol-function 'clutch--allocate-local-port)
               (lambda () 40124))
              ((symbol-function 'make-process)
               (lambda (&rest args)
                 (setq make-process-args args)
                 'fake-proc))
              ((symbol-function 'set-process-query-on-exit-flag) #'ignore)
              ((symbol-function 'clutch--wait-for-ssh-tunnel) #'ignore))
      (clutch--start-tramp-tcp-forward
       '(:backend pg
         :host "db"
         :port 5432
         :tramp-default-directory "/rpc:jump|rpc:devbox:/workspace/"))
      (should (equal (plist-get make-process-args :command)
                     '("ssh"
                       "-N"
                       "-o" "BatchMode=yes"
                       "-o" "ExitOnForwardFailure=yes"
                       "-L" "127.0.0.1:40124:db:5432"
                       "-J" "jump"
                       "devbox"))))))

(ert-deftest clutch-test-start-tramp-forward-starts-docker-container-relay ()
  "Docker TRAMP forward startup should create a local relay listener."
  (let (network-args puts query-flag)
    (cl-letf (((symbol-function 'executable-find)
               (lambda (program)
                 (when (equal program "docker") "/usr/bin/docker")))
              ((symbol-function 'make-network-process)
               (lambda (&rest args)
                 (setq network-args args)
                 'fake-listener))
              ((symbol-function 'process-contact)
               (lambda (_process &optional key _no-block)
                 (when (eq key :service) 40125)))
              ((symbol-function 'process-put)
               (lambda (process key value)
                 (push (list process key value) puts)))
              ((symbol-function 'set-process-query-on-exit-flag)
               (lambda (proc flag)
                 (setq query-flag (list proc flag)))))
      (let ((transport (clutch--start-tramp-tcp-forward
                        '(:backend pg
                          :host "db"
                          :port 5432
                          :tramp-default-directory
                          "/docker:vscode@app:/workspace/"))))
        (should (equal (plist-get network-args :name)
                       "clutch-tramp-container-db:5432"))
        (should (eq (plist-get network-args :server) t))
        (should (equal (plist-get network-args :host) "127.0.0.1"))
        (should (eq (plist-get network-args :service) t))
        (should (eq (plist-get network-args :coding) 'no-conversion))
        (should (equal query-flag '(fake-listener nil)))
        (should (member
                 (list 'fake-listener
                       :clutch-container-command
                       (list "docker" "exec" "-i" "-u" "vscode" "app"
                             "sh" "-lc" clutch--container-relay-script
                             "clutch-container-relay" "db" "5432"))
                 puts))
        (should (eq (plist-get transport :kind) 'tramp))
        (should (eq (plist-get transport :process) 'fake-listener))
        (should (= (plist-get transport :local-port) 40125))
        (should (equal (plist-get transport :tramp-default-directory)
                       "/docker:vscode@app:/workspace/"))))))

(ert-deftest clutch-test-start-tramp-forward-starts-remote-podman-container-relay ()
  "Container TRAMP paths behind SSH hops should run the runtime remotely."
  (let (network-args puts)
    (cl-letf (((symbol-function 'executable-find)
               (lambda (program)
                 (when (equal program "ssh") "/usr/bin/ssh")))
              ((symbol-function 'make-network-process)
               (lambda (&rest args)
                 (setq network-args args)
                 'fake-listener))
              ((symbol-function 'process-contact)
               (lambda (_process &optional key _no-block)
                 (when (eq key :service) 40126)))
              ((symbol-function 'process-put)
               (lambda (process key value)
                 (push (list process key value) puts)))
              ((symbol-function 'set-process-query-on-exit-flag) #'ignore))
      (clutch--start-tramp-tcp-forward
       '(:backend pg
         :host "db"
         :port 5432
         :tramp-default-directory "/ssh:devbox|podman:app:/workspace/"))
      (should (equal (plist-get network-args :name)
                     "clutch-tramp-container-db:5432"))
      (should (member
               (list 'fake-listener
                     :clutch-container-command
                     (append
                      (list "ssh" "-T" "-o" "BatchMode=yes" "devbox")
                      (mapcar
                       #'shell-quote-argument
                       (list "podman" "exec" "-i" "app" "sh" "-lc"
                             clutch--container-relay-script
                             "clutch-container-relay" "db" "5432"))))
               puts)))))

(ert-deftest clutch-test-container-forward-relays-bytes ()
  "Container relay listener should bridge bytes between client and relay."
  (skip-unless (executable-find "cat"))
  (let (server client received)
    (unwind-protect
        (progn
          (setq server
                (make-network-process
                 :name "clutch-test-container-forward"
                 :server t
                 :host "127.0.0.1"
                 :service t
                 :family 'ipv4
                 :coding 'no-conversion
                 :filter #'clutch--container-forward-client-filter
                 :sentinel #'clutch--container-forward-client-sentinel
                 :noquery t))
          (set-process-query-on-exit-flag server nil)
          (process-put server :clutch-container-command (list "cat"))
          (process-put server :clutch-container-listener server)
          (process-put server :clutch-container-children nil)
          (setq client
                (make-network-process
                 :name "clutch-test-container-client"
                 :host "127.0.0.1"
                 :service (process-contact server :service)
                 :family 'ipv4
                 :coding 'no-conversion
                 :filter (lambda (_proc string)
                           (setq received (concat received string)))
                 :noquery t))
          (set-process-query-on-exit-flag client nil)
          (process-send-string client "ping")
          (let ((deadline (+ (float-time) 2)))
            (while (and (< (float-time) deadline)
                        (not (equal received "ping")))
              (accept-process-output nil 0.05)))
          (should (equal received "ping")))
      (when (and client (process-live-p client))
        (delete-process client))
      (when server
        (clutch--stop-connection-transport (list :process server))))))

(ert-deftest clutch-test-ssh-diagnose-output-hints-common-auth-failures ()
  "SSH diagnosis should point users at the right interactive recovery path."
  (dolist (case `((locked-key
                   ,(concat "sign_and_send_pubkey: signing failed for ED25519 "
                            "\"~/.ssh/id_ed25519\" from agent: agent refused operation\n")
                   ("locked" "clutch-prepare-ssh-host"))
                  (authorized-keys
                   "lucius@db: Permission denied (publickey,password).\n"
                   ("clutch-prepare-ssh-host" "authorized_keys"))))
    (pcase-let ((`(,label ,output ,patterns) case))
      (ert-info ((format "case: %s" label))
        (let ((message (clutch--ssh-diagnose-output "bastion-prod" output)))
          (dolist (pattern patterns)
            (should (string-match-p pattern message))))))))

(ert-deftest clutch-test-prepare-ssh-host-starts-interactive-session ()
  "Interactive SSH prepare should open a comint session for the alias."
  (let (started puts sentinel-set query-flag popped message-text proc-lookups)
    (cl-letf (((symbol-function 'executable-find)
               (lambda (_name) "/usr/bin/ssh"))
              ((symbol-function 'make-comint-in-buffer)
               (lambda (name buffer program startfile &rest switches)
                 (setq started (list name buffer program startfile switches))
                 (get-buffer-create buffer)))
              ((symbol-function 'get-buffer-process)
               (lambda (_buffer)
                 (prog1 (if proc-lookups 'fake-proc nil)
                   (setq proc-lookups t))))
              ((symbol-function 'process-live-p)
               (lambda (proc) (eq proc 'fake-proc)))
              ((symbol-function 'process-put)
               (lambda (proc key value)
                 (push (list proc key value) puts)))
              ((symbol-function 'set-process-query-on-exit-flag)
               (lambda (proc flag)
                 (setq query-flag (list proc flag))))
              ((symbol-function 'set-process-sentinel)
               (lambda (proc fn)
                 (setq sentinel-set (list proc fn))))
              ((symbol-function 'pop-to-buffer)
               (lambda (buffer &rest _args)
                 (setq popped buffer)
                 buffer))
              ((symbol-function 'message)
               (lambda (fmt &rest args)
                 (setq message-text (apply #'format fmt args)))))
      (clutch-prepare-ssh-host "bastion-prod")
      (should (equal (car started) "clutch-ssh-prepare-bastion-prod"))
      (should (equal (buffer-name (cadr started))
                     "*clutch-ssh-prepare bastion-prod*"))
      (should (equal (cddr started)
                     '("ssh"
                       nil
                       ("bastion-prod" "exit"))))
      (should (equal query-flag '(fake-proc nil)))
      (should (equal (car puts) '(fake-proc :clutch-ssh-host "bastion-prod")))
      (should (eq (car sentinel-set) 'fake-proc))
      (should (eq (cadr sentinel-set) #'clutch--ssh-prepare-sentinel))
      (should (equal (buffer-name popped) "*clutch-ssh-prepare bastion-prod*"))
      (should (string-match-p "Complete any SSH prompts" message-text)))))

(ert-deftest clutch-test-ssh-prepare-sentinel-nonzero-suggests-retry ()
  "Non-zero SSH prepare exit should not imply preparation was useless."
  (let ((buffer (get-buffer-create "*clutch-ssh-prepare bastion-prod*"))
        message-text)
    (unwind-protect
        (cl-letf (((symbol-function 'process-status)
                   (lambda (_proc) 'exit))
                  ((symbol-function 'process-exit-status)
                   (lambda (_proc) 255))
                  ((symbol-function 'process-get)
                   (lambda (_proc key)
                     (when (eq key :clutch-ssh-host) "bastion-prod")))
                  ((symbol-function 'process-buffer)
                   (lambda (_proc) buffer))
                  ((symbol-function 'message)
                   (lambda (fmt &rest args)
                     (setq message-text (apply #'format fmt args)))))
          (clutch--ssh-prepare-sentinel 'fake-proc "exited")
          (should (string-match-p "retry clutch-connect" message-text))
          (should (string-match-p "inspect" message-text)))
      (kill-buffer buffer))))

(ert-deftest clutch-test-build-conn-errors-early-when-jdbc-pass-entry-is-unresolved ()
  "JDBC connections should fail fast when :pass-entry resolves to no password."
  (let (connect-called)
    (cl-letf (((symbol-function 'clutch--resolve-password)
               (lambda (_params) nil))
              ((symbol-function 'clutch-db-connect)
               (lambda (&rest _args)
                 (setq connect-called t)
                 'fake-conn)))
      (should-error
       (clutch--build-conn
       '(:backend oracle :host "db" :port 1521 :user "u" :sid "orcl"
          :pass-entry "prod-oracle"))
       :type 'user-error)
      (should-not connect-called))))

(ert-deftest clutch-test-build-conn-allows-passwordless-mongodb-pass-entry ()
  "Saved passwordless MongoDB connections should not require auth-source."
  (let (captured-backend captured-params)
    (cl-letf (((symbol-function 'clutch--resolve-password)
               (lambda (_params) nil))
              ((symbol-function 'clutch-db-connect)
               (lambda (backend params)
                 (setq captured-backend backend
                       captured-params params)
                 'fake-conn))
              ((symbol-function 'clutch--connection-alive-p)
               (lambda (_conn) t)))
      (should (eq (clutch--build-conn
                   '(:backend mongodb
                     :host "127.0.0.1"
                     :port 27017
                     :database "app"
                     :pass-entry "mongdb"))
                  'fake-conn))
      (should (eq captured-backend 'mongodb))
      (should (equal captured-params
                     '(:host "127.0.0.1"
                       :port 27017
                       :database "app"))))))

(ert-deftest clutch-test-build-conn-materializes-mongodb-pass-entry ()
  "Authenticated native MongoDB should receive resolved pass-entry passwords."
  (let (captured-backend captured-params)
    (cl-letf (((symbol-function 'clutch--resolve-password)
               (lambda (_params) "secret"))
              ((symbol-function 'clutch-db-connect)
               (lambda (backend params)
                 (setq captured-backend backend
                       captured-params params)
                 'fake-conn))
              ((symbol-function 'clutch--connection-alive-p)
               (lambda (_conn) t)))
      (should (eq (clutch--build-conn
                   '(:backend mongodb
                     :host "127.0.0.1"
                     :port 27017
                     :database "app"
                     :auth-database "admin"
                     :user "root"
                     :pass-entry "mongo-root"))
                  'fake-conn))
      (should (eq captured-backend 'mongodb))
      (should (equal captured-params
                     '(:host "127.0.0.1"
                       :port 27017
                       :database "app"
                       :auth-database "admin"
                       :user "root"
                       :password "secret"))))))

(ert-deftest clutch-test-resolve-password-errors-when-pass-entry-cannot-be-read ()
  "Unreadable pass entries should fail before the backend sees auth."
  (let (connect-called)
    (cl-letf (((symbol-function 'clutch--pass-entry-by-suffix)
               (lambda (_suffix) "mysql/app-prod"))
              ((symbol-function 'auth-source-pass-parse-entry)
               (lambda (_entry) nil))
              ((symbol-function 'clutch-db-connect)
               (lambda (&rest _args)
                 (setq connect-called t)
                 'fake-conn)))
      (let ((err (should-error
                  (clutch--build-conn
                   '(:backend mysql :host "db" :port 3306 :user "u"
                     :database "app" :pass-entry "app-prod"))
                  :type 'user-error)))
        (should (equal
                 (cadr err)
                 "Database password lookup failed for pass entry mysql/app-prod. Unlock pass/auth-source-pass and retry")))
      (should-not connect-called))))

(ert-deftest clutch-test-resolve-password-errors-when-auth-source-secret-fails ()
  "auth-source secret retrieval failures should surface directly."
  (let ((err (cl-letf (((symbol-function 'clutch--pass-entry-by-suffix)
                        (lambda (_suffix) nil))
                       ((symbol-function 'auth-source-search)
                        (lambda (&rest _args)
                          (list (list :secret (lambda ()
                                                (error "bad decrypt")))))))
               (should-error
                (clutch--resolve-password
                 '(:host "db.example.com" :port 3306 :user "scott"))
                :type 'user-error))))
    (should (equal
             (cadr err)
             "Database password lookup failed via auth-source for scott@db.example.com:3306: bad decrypt"))))

(ert-deftest clutch-test-build-conn-relays-db-errors-without-duplicate-prefixes ()
  "User-facing connect errors should not duplicate nested connection-failed wrappers."
  (let ((err (cl-letf (((symbol-function 'clutch--resolve-password)
                        (lambda (_params) nil))
                       ((symbol-function 'clutch-db-connect)
                        (lambda (&rest _args)
                          (signal 'clutch-db-error
                                  '("Connection failed (oracle): Connection attempt timed out")))))
               (should-error
                (clutch--build-conn '(:backend oracle :host "db" :port 1521 :user "u"))
                :type 'user-error))))
    (should (string-match-p
             "Connection failed [(]oracle[)]: Connection attempt timed out"
             (cadr err)))
    (should (string-match-p "clutch-debug-mode" (cadr err)))
    (should (string-match-p (regexp-quote clutch-debug-buffer-name)
                            (cadr err)))))

(ert-deftest clutch-test-build-conn-wraps-ssh-setup-errors-before-user-display ()
  "SSH setup errors should be normalized before they reach the minibuffer."
  (let ((err (cl-letf (((symbol-function 'clutch--materialize-connection-params)
                        (lambda (params) params))
                       ((symbol-function 'clutch--prepare-connect-params)
                        (lambda (_params)
                          (signal 'clutch-db-error
                                  '("SSH tunnel to arch failed: SSH authentication to arch was rejected")))))
               (should-error
                (clutch--build-conn '(:backend mysql :host "db" :port 3306 :ssh-host "arch"))
                :type 'user-error))))
    (should (string-match-p
             "SSH tunnel to arch failed: SSH authentication to arch was rejected"
             (cadr err)))
    (should-not (string-match-p "^let\\*:" (cadr err)))
    (should-not (string-match-p "^if:" (cadr err)))
    (should-not (string-match-p "^when-let\\*:" (cadr err)))))

(ert-deftest clutch-test-build-conn-enriches-buffer-error-details ()
  "Connect failures should keep enriched `current-buffer' error details."
  (with-temp-buffer
    (let ((raw-message "Connection refused (host=db.example.com, port=3306)"))
      (cl-letf (((symbol-function 'clutch--resolve-password)
                 (lambda (_params) nil))
                ((symbol-function 'clutch-db-connect)
                 (lambda (&rest _args)
                   (signal 'clutch-db-error (list raw-message)))))
        (let ((signaled
               (should-error
                (clutch--build-conn
                 '(:backend oracle :host "db.example.com" :port 1521
                   :user "scott" :database "orcl"))
                :type 'user-error)))
          (should (equal (cadr signaled)
                         (clutch--debug-workflow-message
                          (clutch--humanize-db-error raw-message))))
          (let* ((details clutch--buffer-error-details)
                 (diag (plist-get details :diag))
                 (context (plist-get diag :context)))
            (should details)
            (should (eq (plist-get details :backend) 'oracle))
            (should (equal (plist-get details :summary)
                           (clutch--humanize-db-error raw-message)))
            (should (equal (plist-get diag :raw-message) raw-message))
            (should (equal (plist-get context :host) "db.example.com"))
            (should (equal (plist-get context :port) 1521))
            (should (equal (plist-get context :database) "orcl"))
            (should (eq (plist-get context :backend) 'oracle))))))))

(ert-deftest clutch-test-build-conn-skips-timeouts-for-sqlite ()
  "Test that `clutch--build-conn' does not pass network timeout keys to sqlite."
  (let ((clutch-connect-timeout-seconds 11)
        (clutch-read-idle-timeout-seconds 42)
        captured)
    (cl-letf (((symbol-function 'clutch--resolve-password)
               (lambda (_params) nil))
              ((symbol-function 'clutch-db-connect)
               (lambda (_backend params)
                 (setq captured params)
                 'fake-conn)))
      (clutch--build-conn '(:backend sqlite :database ":memory:"))
      (should-not (plist-member captured :connect-timeout))
      (should-not (plist-member captured :read-idle-timeout))
      (should-not (plist-member captured :query-timeout))
      (should-not (plist-member captured :rpc-timeout)))))

(ert-deftest clutch-test-build-conn-rejects-removed-read-timeout ()
  "Test that removed timeout keys fail fast with a clear error."
  (should-error
   (clutch--build-conn '(:backend mysql :host "127.0.0.1" :port 3306
                         :user "u" :read-timeout 5))
   :type 'user-error))

(ert-deftest clutch-test-effective-sql-product-returns-nil-without-backend ()
  "Connections without :backend should not infer a SQL product."
  (should-not (clutch--effective-sql-product '(:host "127.0.0.1"
                                               :port 3306
                                               :user "u"
                                               :database "db"))))

(ert-deftest clutch-test-effective-sql-product-uses-backend-registry ()
  "Connection SQL products should be derived from backend metadata."
  (should (eq (clutch--effective-sql-product '(:backend pg)) 'postgres))
  (should (eq (clutch--effective-sql-product '(:backend oracle)) 'oracle))
  (should (eq (clutch--effective-sql-product
               '(:backend clickhouse :sql-product mysql))
              'mysql)))

(ert-deftest clutch-test-build-conn-errors-without-backend ()
  "Building a connection should fail fast when :backend is missing."
  (let ((err (should-error
              (clutch--build-conn
               '(:host "127.0.0.1" :port 3306 :user "u" :database "db"))
              :type 'user-error)))
    (should (string-match-p ":backend" (cadr err)))))

(ert-deftest clutch-test-default-connect-timeout-is-10-seconds ()
  "Project default connect timeout should stay at 10 seconds."
  (should (= clutch-connect-timeout-seconds 10)))

(ert-deftest clutch-test-build-conn-failure-points-to-debug-workflow-when-disabled ()
  "Connect failures should tell users how to capture a debug trace."
  (let ((err (cl-letf (((symbol-function 'clutch-db-connect)
                        (lambda (_backend _params)
                          (signal 'clutch-db-error
                                  '("Connection refused (host=db.example.com, port=3306)")))))
               (should-error
                (clutch--build-conn '(:backend mysql :host "db.example.com" :port 3306))
                :type 'user-error))))
    (should (string-match-p "check host and port" (cadr err)))
    (should (string-match-p "clutch-debug-mode" (cadr err)))
    (should (string-match-p (regexp-quote clutch-debug-buffer-name)
                            (cadr err)))))

(ert-deftest clutch-test-build-conn-failure-points-to-debug-buffer-when-enabled ()
  "Connect failures should point directly at the debug buffer when capture is on."
  (let ((clutch-debug-mode t))
    (let ((err (cl-letf (((symbol-function 'clutch-db-connect)
                          (lambda (_backend _params)
                            (signal 'clutch-db-error
                                    '("Connection refused (host=db.example.com, port=3306)")))))
                 (should-error
                  (clutch--build-conn '(:backend mysql :host "db.example.com" :port 3306))
                  :type 'user-error))))
      (should (string-match-p "check host and port" (cadr err)))
      (should-not (string-match-p "clutch-debug-mode" (cadr err)))
      (should (string-match-p (regexp-quote clutch-debug-buffer-name)
                              (cadr err))))))

(ert-deftest clutch-test-build-conn-jdbc-driver-missing-points-to-install-command ()
  "Missing JDBC driver should point to install-driver, not the debug workflow."
  (let ((err (cl-letf (((symbol-function 'clutch-db-connect)
                        (lambda (_backend _params)
                          (signal 'clutch-db-error
                                  '("SQLException [SQLState=08001]: No suitable driver found for jdbc:oracle:thin:@//db:1521/ORCL")))))
               (should-error
                (clutch--build-conn '(:backend oracle :driver jdbc :host "db" :port 1521))
                :type 'user-error))))
    (should (string-match-p "clutch-jdbc-install-driver RET oracle" (cadr err)))
    (should-not (string-match-p "clutch-debug-mode" (cadr err)))
    (should-not (string-match-p (regexp-quote clutch-debug-buffer-name)
                                (cadr err)))))

(ert-deftest clutch-test-build-conn-agent-missing-points-to-ensure-agent ()
  "Missing JDBC agent should point to ensure-agent, not the debug workflow."
  (let ((err (cl-letf (((symbol-function 'clutch-db-connect)
                        (lambda (_backend _params)
                          (signal 'clutch-db-error
                                  '("JDBC agent jar not found: /tmp/clutch-jdbc-agent.jar\nRun M-x clutch-jdbc-ensure-agent")))))
               (should-error
                (clutch--build-conn '(:backend oracle :driver jdbc :host "db" :port 1521))
                :type 'user-error))))
    (should (string-match-p "Run M-x clutch-jdbc-ensure-agent" (cadr err)))
    (should-not (string-match-p "clutch-debug-mode" (cadr err)))
    (should-not (string-match-p (regexp-quote clutch-debug-buffer-name)
                                (cadr err)))))

(ert-deftest clutch-test-readers-load-clutch-entrypoint ()
  "Saved-connection readers should load `clutch' before checking profiles."
  (dolist (case `((clutch--read-connection-params
                   (:backend mysql :database "app_a" :pass-entry "alpha"))
                  (clutch--read-query-console-target
                   "alpha")))
    (pcase-let ((`(,reader ,expected) case))
      (ert-info ((symbol-name reader))
        (let ((clutch-connection-alist nil)
              required
              (orig-featurep (symbol-function 'featurep))
              (orig-require (symbol-function 'require)))
          (cl-letf (((symbol-function 'featurep)
                     (lambda (feature &optional subfeature)
                       (if (eq feature 'clutch)
                           nil
                         (funcall orig-featurep feature subfeature))))
                    ((symbol-function 'require)
                     (lambda (feature &optional filename noerror)
                       (if (eq feature 'clutch)
                           (progn
                             (setq required t
                                   clutch-connection-alist
                                   '(("alpha" . (:backend mysql :database "app_a"))))
                             t)
                         (funcall orig-require feature filename noerror))))
                    ((symbol-function 'completing-read)
                     (lambda (&rest _args) "alpha")))
            (should (equal (funcall reader) expected))
            (should required)))))))

(ert-deftest clutch-test-read-connection-params-prompts-for-backend-when-unsaved ()
  "Manual connection prompts should collect an explicit backend first."
  (let ((clutch-connection-alist nil)
        backend-prompt
        port-default)
    (cl-letf (((symbol-function 'completing-read)
               (lambda (prompt _collection &rest _args)
                 (setq backend-prompt prompt)
                 "pg"))
              ((symbol-function 'read-string)
               (lambda (prompt &optional _initial _history default-value _inherit)
                 (pcase prompt
                   ("Host (127.0.0.1): " "db.example.com")
                   ("User: " "alice")
                   ("SSH host from ~/.ssh/config (optional): " "bastion-prod")
                   ("Database (optional): " "app_db")
                   (_ (or default-value "")))))
              ((symbol-function 'read-number)
               (lambda (_prompt default)
                 (setq port-default default)
                 5544))
              ((symbol-function 'clutch--resolve-password)
               (lambda (_params) nil))
              ((symbol-function 'read-passwd)
               (lambda (&rest _args) "secret")))
      (should (equal (clutch--read-connection-params)
                     '(:backend pg
                       :host "db.example.com"
                       :port 5544
                       :user "alice"
                       :ssh-host "bastion-prod"
                       :password "secret"
                       :database "app_db")))
      (should (string-match-p "Backend" backend-prompt))
      (should (= port-default 5432)))))

(ert-deftest clutch-test-read-connection-params-uses-clickhouse-registry-port ()
  "Manual ClickHouse connections should use the backend registry default port."
  (let ((clutch-connection-alist nil)
        port-default)
    (cl-letf (((symbol-function 'completing-read)
               (lambda (&rest _args) "clickhouse"))
              ((symbol-function 'read-string)
               (lambda (prompt &optional _initial _history default-value _inherit)
                 (pcase prompt
                   ("Host (127.0.0.1): " "127.0.0.1")
                   ("User: " "default")
                   ("SSH host from ~/.ssh/config (optional): " "")
                   ("Database (optional): " "default")
                   (_ (or default-value "")))))
              ((symbol-function 'read-number)
               (lambda (_prompt default)
                 (setq port-default default)
                 default))
              ((symbol-function 'clutch--resolve-password)
               (lambda (_params) nil))
              ((symbol-function 'read-passwd)
               (lambda (&rest _args) "")))
      (should (equal (clutch--read-connection-params)
                     '(:backend clickhouse
                       :host "127.0.0.1"
                       :port 8123
                       :user "default"
                       :password ""
                       :database "default")))
      (should (= port-default 8123)))))

(ert-deftest clutch-test-read-connection-params-uses-mongodb-registry-port ()
  "Manual MongoDB connections should use the backend registry default port."
  (let ((clutch-connection-alist nil)
        port-default)
    (cl-letf (((symbol-function 'completing-read)
               (lambda (&rest _args) "mongodb"))
              ((symbol-function 'read-string)
               (lambda (prompt &optional _initial _history default-value _inherit)
                 (pcase prompt
                   ("Host (127.0.0.1): " "cluster0.a.query.mongodb.net")
                   ("User: " "reporter")
                   ("SSH host from ~/.ssh/config (optional): " "")
                   ("Database (optional): " "analytics")
                   (_ (or default-value "")))))
              ((symbol-function 'read-number)
               (lambda (_prompt default)
                 (setq port-default default)
                 default))
              ((symbol-function 'clutch--resolve-password)
               (lambda (_params) nil))
              ((symbol-function 'read-passwd)
               (lambda (&rest _args) "secret")))
      (should (equal (clutch--read-connection-params)
                     '(:backend mongodb
                       :host "cluster0.a.query.mongodb.net"
                       :port 27017
                       :user "reporter"
                       :password "secret"
                       :database "analytics")))
      (should (= port-default 27017)))))

(ert-deftest clutch-test-canonicalize-connection-params-normalizes-mongo-alias ()
  "The public mongo alias should normalize to the registered mongodb backend."
  (should (equal (clutch--canonicalize-connection-params
                  '(:backend mongo :database "analytics"))
                 '(:backend mongodb :database "analytics"))))

(ert-deftest clutch-test-canonicalize-connection-params-normalizes-mongodb-surface-sql-surface ()
  "MongoDB SQL Interface should be a surface on the MongoDB backend."
  (should (equal (clutch--canonicalize-connection-params
                  '(:backend mongo :surface "sql-interface" :database "analytics"))
                 '(:backend mongodb :surface sql-interface :database "analytics"))))

(ert-deftest clutch-test-canonicalize-connection-params-rejects-mongodb-driver-option ()
  "MongoDB should not accept a public driver option."
  (dolist (driver '(mongo mongodb jdbc))
    (should-error
     (clutch--canonicalize-connection-params
      `(:backend mongodb :driver ,driver :database "analytics"))
     :type 'user-error)))

;;;; Connection — transaction and auto-commit

(ert-deftest clutch-test-tx-header-line-and-update ()
  "Tx header-line uses semantic colors and dirty adds *."
  (let ((clutch--tx-dirty-cache (make-hash-table :test 'eq)))
    (with-temp-buffer
      (clutch-mode)
      (setq-local clutch-connection 'fake-conn)
      (cl-letf (((symbol-function 'clutch-db-display-name)
                 (lambda (_conn) "Oracle"))
                ((symbol-function 'clutch--connection-display-key)
                 (lambda (_conn) "scott@db"))
                ((symbol-function 'clutch--connection-alive-p)
                 (lambda (_conn) t))
                ((symbol-function 'clutch-db-manual-commit-supported-p)
                 (lambda (_conn) t))
                ((symbol-function 'clutch-db-manual-commit-p)
                 (lambda (_conn) t))
                ((symbol-function 'clutch--schema-status-header-line-segment)
                 (lambda (_conn) nil))
                ((symbol-function 'clutch--icon)
                 (lambda (_spec &rest _fb) "[lock]")))
        ;; Mode-line is now just the mode name.
        (clutch--update-mode-line)
        (should (equal mode-name "clutch"))
        (should (equal header-line-format
                       '((:eval (clutch--build-connection-header-line)))))
        ;; Auto-commit: shows Tx: Auto in success face.
        (cl-letf (((symbol-function 'clutch-db-manual-commit-p)
                   (lambda (_conn) nil)))
          (let ((seg (clutch--tx-header-line-segment clutch-connection)))
            (should (string-match-p "Tx: Auto" seg))
            (should (eq (get-text-property 0 'face seg) 'success))))
        ;; header-line-format is an (:eval ...) form; evaluate it to get content.
        (let ((hl (clutch--build-connection-header-line)))
          ;; Clean manual-commit: shows Tx: Manual (no asterisk).
          (should (string-match-p "Tx: Manual" hl))
          (should-not (string-match-p "Tx: Manual\\*" hl)))
        (let ((seg (clutch--tx-header-line-segment clutch-connection)))
          (should (eq (get-text-property 0 'face seg) 'warning)))
        ;; Dirty: header-line shows Tx: Manual*.
        (puthash clutch-connection t clutch--tx-dirty-cache)
        (let ((hl (clutch--build-connection-header-line))
              (seg (clutch--tx-header-line-segment clutch-connection)))
          (should (string-match-p "Tx: Manual\\*" hl))
          (should (eq (get-text-property 0 'face seg) 'error)))))))

(ert-deftest clutch-test-update-mode-line-shows-spinner-when-executing ()
  "Busy buffers should show the current spinner frame in `mode-name'."
  (with-temp-buffer
    (clutch-mode)
    (let ((clutch--executing-p t)
          (clutch--spinner-timer t)
          (clutch--spinner-index 2))
      (clutch--update-mode-line)
      (should (equal mode-name "clutch ⠹"))
      (should-not (string-match-p "\\[\\.\\.\\.\\]" mode-name)))))

(ert-deftest clutch-test-result-footer-shows-spinner-when-executing ()
  "Busy result buffers should show the current spinner frame in the footer."
  (with-temp-buffer
    (clutch-result-mode)
    (let ((clutch--footer-base-string "Σ 1 of ? rows")
          (clutch--executing-p t)
          (clutch--spinner-timer t)
          (clutch--spinner-index 2))
      (clutch--refresh-footer-display)
      (should (string-match-p "⠹"
                              (clutch--footer-mode-line-display))))))

(ert-deftest clutch-test-result-footer-spinner-replaces-elapsed-time ()
  "Busy result buffers should use the elapsed-time slot for the spinner."
  (with-temp-buffer
    (clutch-result-mode)
    (let ((clutch--footer-base-string "Σ 1 of ? rows")
          (clutch--query-elapsed 0.042)
          (clutch--executing-p t)
          (clutch--spinner-timer t)
          (clutch--spinner-index 2))
      (cl-letf (((symbol-function 'clutch--format-elapsed)
                 (lambda (_seconds) "42ms")))
        (clutch--refresh-footer-display)
        (let ((footer (substring-no-properties
                       (clutch--footer-mode-line-display))))
          (should (string-match-p "⏱ +⠹" footer))
          (should-not (string-match-p "42ms" footer)))))))

(ert-deftest clutch-test-result-footer-restores-elapsed-time-when-idle ()
  "Idle result buffers should show elapsed time in the timing slot."
  (with-temp-buffer
    (clutch-result-mode)
    (let ((clutch--footer-base-string "Σ 1 of ? rows")
          (clutch--query-elapsed 0.042)
          (clutch--executing-p nil)
          (clutch--spinner-timer t)
          (clutch--spinner-index 2))
      (cl-letf (((symbol-function 'clutch--format-elapsed)
                 (lambda (_seconds) "42ms")))
        (clutch--refresh-footer-display)
        (let ((footer (substring-no-properties
                       (clutch--footer-mode-line-display))))
          (should (string-match-p "⏱ +42ms" footer))
          (should-not (string-match-p "⠹" footer)))))))

(ert-deftest clutch-test-spinner-tick-stops-when-no-busy-buffers ()
  "Spinner timer should stop itself when no buffers are busy."
  (with-temp-buffer
    (let ((clutch--spinner-timer 'fake-timer)
          (clutch--spinner-index 0)
          cancelled)
      (cl-letf (((symbol-function 'buffer-list)
                 (lambda (&optional _frame) (list (current-buffer))))
                ((symbol-function 'cancel-timer)
                 (lambda (_timer) (setq cancelled t))))
        (clutch--spinner-tick)
        (should cancelled)
        (should-not clutch--spinner-timer)
        (should (zerop clutch--spinner-index))))))

(ert-deftest clutch-test-run-db-query-updates-manual-commit-dirty-state ()
  "Query execution should update dirty state from SQL and backend DDL semantics."
  (dolist (case '((dml "UPDATE demo SET x = 1" nil nil t)
                  (transactional-ddl "CREATE TABLE demo (id int)" dirty nil t)
                  (autocommit-ddl "CREATE TABLE demo (id int)" clear t nil)
                  (commit "COMMIT" nil t nil)
                  (unknown-ddl "ALTER TABLE demo ADD x NUMBER" nil t t)))
    (pcase-let ((`(,label ,sql ,schema-effect ,initial-dirty ,expected-dirty)
                 case))
      (ert-info ((format "case: %s" label))
        (let ((clutch--tx-dirty-cache (make-hash-table :test 'eq))
              (conn 'fake-conn))
          (when initial-dirty
            (puthash conn t clutch--tx-dirty-cache))
          (cl-letf (((symbol-function 'clutch-db-query)
                     (lambda (_conn _sql) '(:ok t)))
                    ((symbol-function 'clutch-db-manual-commit-p)
                     (lambda (_conn) t))
                    ((symbol-function 'clutch-db-schema-transaction-effect)
                     (lambda (_conn _sql) schema-effect))
                    ((symbol-function 'clutch--refresh-transaction-ui) #'ignore))
            (clutch--run-db-query conn sql)
            (should (eq (not (null (clutch--tx-dirty-p conn)))
                        expected-dirty))))))))

(ert-deftest clutch-test-transaction-commands-error-in-autocommit-mode ()
  "Commit and rollback should error when the connection is not in manual mode."
  (dolist (command '(clutch-commit clutch-rollback))
    (ert-info ((format "command: %s" command))
      (let ((clutch-connection 'fake-conn))
        (cl-letf (((symbol-function 'clutch--ensure-connection) #'ignore)
                  ((symbol-function 'clutch-db-manual-commit-supported-p)
                   (lambda (_conn) t))
                  ((symbol-function 'clutch-db-manual-commit-p)
                   (lambda (_conn) nil)))
          (should-error (funcall command) :type 'user-error))))))

(ert-deftest clutch-test-transaction-commands-call-rpc-and-clear-dirty ()
  "Commit and rollback should fire their RPC and clear the dirty cache."
  (dolist (case '((commit clutch-commit clutch-db-commit)
                  (rollback clutch-rollback clutch-db-rollback)))
    (pcase-let ((`(,label ,command ,rpc) case))
      (ert-info ((format "case: %s" label))
        (let ((clutch--tx-dirty-cache (make-hash-table :test 'eq))
              (clutch-connection 'fake-conn)
              called)
          (puthash clutch-connection t clutch--tx-dirty-cache)
          (cl-letf (((symbol-function 'clutch--ensure-connection) #'ignore)
                    ((symbol-function 'clutch-db-manual-commit-supported-p)
                     (lambda (_conn) t))
                    ((symbol-function 'clutch-db-manual-commit-p)
                     (lambda (_conn) t))
                    ((symbol-function rpc)
                     (lambda (_conn) (setq called t)))
                    ((symbol-function 'clutch--refresh-transaction-ui) #'ignore))
            (funcall command)
            (should called)
            (should-not (clutch--tx-dirty-p clutch-connection))))))))

(defun clutch-test--make-dml-result-buf (conn)
  "Create a temporary DML result buffer associated with CONN for testing."
  (let ((buf (generate-new-buffer " *clutch-dml-test*")))
    (with-current-buffer buf
      (clutch-result-mode)
      (setq-local clutch-connection conn)
      (setq-local clutch--dml-result t)
      (setq-local header-line-format nil))
    buf))

(ert-deftest clutch-test-rollback-marks-dml-result-buffers ()
  "Clutch-rollback should add a warning banner to open DML result buffers."
  (let* ((clutch--tx-dirty-cache (make-hash-table :test 'eq))
         (clutch-connection 'fake-conn)
         (buf (clutch-test--make-dml-result-buf 'fake-conn)))
    (puthash clutch-connection t clutch--tx-dirty-cache)
    (unwind-protect
        (cl-letf (((symbol-function 'clutch--ensure-connection) #'ignore)
                  ((symbol-function 'clutch-db-manual-commit-supported-p)
                   (lambda (_conn) t))
                  ((symbol-function 'clutch-db-manual-commit-p) (lambda (_conn) t))
                  ((symbol-function 'clutch-db-rollback) #'ignore)
                  ((symbol-function 'clutch--refresh-transaction-ui) #'ignore))
          (clutch-rollback)
          (should (with-current-buffer buf header-line-format))
          (should (string-match-p "rolled back"
                                  (with-current-buffer buf
                                    (substring-no-properties header-line-format)))))
      (kill-buffer buf))))

(ert-deftest clutch-test-commit-marks-dml-result-buffers ()
  "Clutch-commit should add a committed banner to open DML result buffers."
  (let* ((clutch--tx-dirty-cache (make-hash-table :test 'eq))
         (clutch-connection 'fake-conn)
         (buf (clutch-test--make-dml-result-buf 'fake-conn)))
    (unwind-protect
        (cl-letf (((symbol-function 'clutch--ensure-connection) #'ignore)
                  ((symbol-function 'clutch-db-manual-commit-supported-p)
                   (lambda (_conn) t))
                  ((symbol-function 'clutch-db-manual-commit-p) (lambda (_conn) t))
                  ((symbol-function 'clutch-db-commit) #'ignore)
                  ((symbol-function 'clutch--refresh-transaction-ui) #'ignore))
          (clutch-commit)
          (should (string-match-p "committed"
                                  (with-current-buffer buf
                                    (substring-no-properties header-line-format)))))
      (kill-buffer buf))))

(ert-deftest clutch-test-disconnect-marks-dml-result-buffers ()
  "Clutch-disconnect should add a connection-closed notice to open DML result buffers."
  (let* ((clutch--tx-dirty-cache (make-hash-table :test 'eq))
         (clutch-connection 'fake-conn)
         (buf (clutch-test--make-dml-result-buf 'fake-conn)))
    (unwind-protect
        (cl-letf (((symbol-function 'clutch--connection-alive-p) (lambda (_c) t))
                  ((symbol-function 'clutch--confirm-disconnect-transaction-loss) #'ignore)
                  ((symbol-function 'clutch-db-disconnect) #'ignore)
                  ((symbol-function 'clutch--refresh-transaction-ui) #'ignore)
                  ((symbol-function 'clutch--update-console-buffer-name) #'ignore)
                  ((symbol-function 'clutch--update-mode-line) #'ignore))
          (clutch-disconnect)
          (should (string-match-p "closed"
                                  (with-current-buffer buf
                                    (substring-no-properties header-line-format)))))
      (kill-buffer buf))))

(ert-deftest clutch-test-toggle-auto-commit-manual-to-auto ()
  "Toggling from manual→auto (clean) calls set-auto-commit(t) and clears dirty."
  (let ((clutch--tx-dirty-cache (make-hash-table :test 'eq))
        (clutch-connection 'fake-conn)
        captured-auto-commit
        mode-line-updated)
    (cl-letf (((symbol-function 'clutch--ensure-connection) #'ignore)
              ((symbol-function 'clutch-db-manual-commit-supported-p)
               (lambda (_conn) t))
              ((symbol-function 'clutch-db-manual-commit-p) (lambda (_conn) t))
              ((symbol-function 'clutch-db-set-auto-commit)
               (lambda (_conn v) (setq captured-auto-commit v)))
              ((symbol-function 'clutch--update-mode-line)
               (lambda () (setq mode-line-updated t))))
      (clutch-toggle-auto-commit)
      (should (eq t captured-auto-commit))
      (should mode-line-updated))))

(ert-deftest clutch-test-toggle-auto-commit-auto-to-manual ()
  "Toggling from auto→manual calls set-auto-commit(nil), does not clear dirty."
  (let ((clutch--tx-dirty-cache (make-hash-table :test 'eq))
        (clutch-connection 'fake-conn)
        captured-auto-commit)
    (cl-letf (((symbol-function 'clutch--ensure-connection) #'ignore)
              ((symbol-function 'clutch-db-manual-commit-supported-p)
               (lambda (_conn) t))
              ((symbol-function 'clutch-db-manual-commit-p) (lambda (_conn) nil))
              ((symbol-function 'clutch-db-set-auto-commit)
               (lambda (_conn v) (setq captured-auto-commit v)))
              ((symbol-function 'clutch--update-mode-line) #'ignore))
      (clutch-toggle-auto-commit)
      (should (eq nil captured-auto-commit)))))

(ert-deftest clutch-test-toggle-auto-commit-blocked-when-dirty ()
  "When dirty, the toggle must not proceed (no confirmation, immediate error)."
  (let ((clutch--tx-dirty-cache (make-hash-table :test 'eq))
        (clutch-connection 'fake-conn)
        toggle-called)
    (puthash clutch-connection t clutch--tx-dirty-cache)
    (cl-letf (((symbol-function 'clutch--ensure-connection) #'ignore)
              ((symbol-function 'clutch-db-manual-commit-supported-p)
               (lambda (_conn) t))
              ((symbol-function 'clutch-db-manual-commit-p) (lambda (_conn) t))
              ((symbol-function 'clutch-db-set-auto-commit)
               (lambda (_conn _v) (setq toggle-called t))))
      (should-error (clutch-toggle-auto-commit) :type 'user-error)
      (should-not toggle-called)
      (should (clutch--tx-dirty-p clutch-connection)))))

(ert-deftest clutch-test-toggle-auto-commit-errors-when-unsupported ()
  "Backends without manual commit support should not attempt a toggle."
  (let ((clutch-connection 'fake-conn)
        toggle-called)
    (cl-letf (((symbol-function 'clutch--ensure-connection) #'ignore)
              ((symbol-function 'clutch-db-manual-commit-supported-p)
               (lambda (_conn) nil))
              ((symbol-function 'clutch-db-set-auto-commit)
               (lambda (_conn _v) (setq toggle-called t))))
      (should-error (clutch-toggle-auto-commit) :type 'user-error)
      (should-not toggle-called))))

(ert-deftest clutch-test-sqlite-omits-manual-commit-ui ()
  "SQLite should not advertise Clutch manual-commit controls."
  (require 'clutch-db-sqlite)
  (let ((conn (make-clutch-db-sqlite-conn :database "/tmp/bookmarks.db")))
    (should-not (clutch-db-manual-commit-supported-p conn))
    (should-not (clutch--manual-commit-supported-p conn))
    (should-not (clutch--tx-header-line-segment conn))))

;;;; Connection — display key and icons

(ert-deftest clutch-test-tx-header-line-segment-preserves-icon-family ()
  "Transaction header icons should keep the nerd-icons font family."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn)
    (cl-letf (((symbol-function 'clutch-db-manual-commit-supported-p)
               (lambda (_conn) t))
              ((symbol-function 'clutch-db-manual-commit-p)
               (lambda (_conn) t))
              ((symbol-function 'clutch--tx-dirty-p)
               (lambda (_conn) nil))
              ((symbol-function 'clutch--icon)
               (lambda (_spec &rest _fallback)
                 (propertize "[lock]"
                             'face '(:family "Symbols Nerd Font Mono")))))
      (let* ((seg (clutch--tx-header-line-segment clutch-connection))
             (face (get-text-property 0 'face seg)))
        (should (equal face '((:family "Symbols Nerd Font Mono") warning)))))))

(ert-deftest clutch-test-icon-with-face-preserves-family-without-mutating-source ()
  "Tinting an icon should preserve its family and leave the source string untouched."
  (let ((source (propertize (copy-sequence "[icon]")
                            'face '(:family "Symbols Nerd Font Mono"))))
    (cl-letf (((symbol-function 'clutch--icon)
               (lambda (_spec &rest _fallback)
                 source)))
      (let* ((icon (clutch--icon-with-face '(codicon . "nf-cod-files")
                                           "⊞"
                                           'font-lock-keyword-face))
             (source-face (get-text-property 0 'face source))
             (icon-face (get-text-property 0 'face icon)))
        (should (equal source-face '(:family "Symbols Nerd Font Mono")))
        (should (equal icon-face
                       '((:family "Symbols Nerd Font Mono")
                         font-lock-keyword-face)))))))

(ert-deftest clutch-test-fixed-width-icon-preserves-icon-family-when-faced ()
  "Fixed-width icons should append FACE without dropping the icon family."
  (cl-letf (((symbol-function 'clutch--icon)
             (lambda (_spec &rest _fallback)
               (propertize "[sort]"
                           'face '(:family "Symbols Nerd Font Mono")))))
    (let* ((icon (clutch--fixed-width-icon '(codicon . "nf-cod-arrow_up")
                                           "▲"
                                           'header-line))
           (face (get-text-property 0 'face icon)))
      (should (equal face
                     '((:family "Symbols Nerd Font Mono")
                       header-line))))))

(ert-deftest clutch-test-icon-dispatches-public-nerd-icons-functions ()
  "Icon helper should dispatch through public nerd-icons render functions."
  (cl-letf (((symbol-function 'require) (lambda (&rest _) t))
            ((symbol-function 'nerd-icons-octicon)
             (lambda (name) (concat "oct:" name)))
            ((symbol-function 'nerd-icons-devicon)
             (lambda (name) (concat "dev:" name))))
    (should (equal (clutch--icon '(octicon . "nf-oct-sort_desc") "fallback")
                   "oct:nf-oct-sort_desc"))
    (should (equal (clutch--icon '(devicon . "nf-dev-mysql") "fallback")
                   "dev:nf-dev-mysql"))))

(ert-deftest clutch-test-connected-header-line-uses-backend-direction-separator ()
  "Connected header line should group backend and connection identity."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn)
    (cl-letf (((symbol-function 'clutch--connection-alive-p) (lambda (_conn) t))
              ((symbol-function 'clutch--connection-backend-segment)
               (lambda (&rest _args) "[db]"))
              ((symbol-function 'clutch--connection-state-icon)
               (lambda (_connected) "[ok]"))
              ((symbol-function 'clutch--connection-display-key)
               (lambda (_conn) "user@host"))
              ((symbol-function 'clutch--current-schema-header-line-segment)
               (lambda (_conn) "[schema] app"))
              ((symbol-function 'clutch--schema-status-header-line-segment)
               (lambda (_conn) nil))
              ((symbol-function 'clutch--tx-header-line-segment)
               (lambda (_conn) "Tx: Auto"))
              ((symbol-function 'clutch--header-line-indent) (lambda () "")))
      (let ((line (substring-no-properties
                   (clutch--build-connection-header-line))))
        (should (equal line
                       "[db]  ›  [ok] user@host  •  [schema] app  •  Tx: Auto"))))))

(ert-deftest clutch-test-header-line-shows-schema-even-when-redundant ()
  "Connection header line should show the effective schema whenever available."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn)
    (cl-letf (((symbol-function 'clutch--connection-alive-p) (lambda (_conn) t))
              ((symbol-function 'clutch--icon) (lambda (&rest _) "[schema]"))
              ((symbol-function 'clutch--db-backend-icon-for-key) (lambda (_key) nil))
              ((symbol-function 'clutch-db-display-name) (lambda (_conn) "MySQL"))
              ((symbol-function 'clutch--connection-display-key) (lambda (_conn) "user@host"))
              ((symbol-function 'clutch-db-user) (lambda (_conn) "user"))
              ((symbol-function 'clutch-db-database) (lambda (_conn) "sales"))
              ((symbol-function 'clutch-db-current-schema) (lambda (_conn) "sales"))
              ((symbol-function 'clutch--schema-status-header-line-segment) (lambda (_conn) nil))
              ((symbol-function 'clutch--tx-header-line-segment) (lambda (_conn) nil))
              ((symbol-function 'clutch--header-line-indent) (lambda () "")))
      (let ((line (clutch--build-connection-header-line)))
        (should (string-match-p "user@host" line))
        (should (string-match-p "\\[schema\\] sales" line))
        (should-not (string-match-p "Schema:" line))))))

(ert-deftest clutch-test-header-line-shows-clickhouse-database ()
  "Connection header line should show the current ClickHouse database."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn)
    (cl-letf (((symbol-function 'clutch--connection-alive-p) (lambda (_conn) t))
              ((symbol-function 'clutch--connection-clickhouse-p) (lambda (_conn) t))
              ((symbol-function 'clutch--icon) (lambda (&rest _) "[schema]"))
              ((symbol-function 'clutch--db-backend-icon-for-key) (lambda (_key) nil))
              ((symbol-function 'clutch-db-display-name) (lambda (_conn) "ClickHouse"))
              ((symbol-function 'clutch--connection-display-key) (lambda (_conn) "default@127.0.0.1"))
              ((symbol-function 'clutch-db-database) (lambda (_conn) "demo"))
              ((symbol-function 'clutch-db-current-schema) (lambda (_conn) nil))
              ((symbol-function 'clutch--schema-status-header-line-segment) (lambda (_conn) nil))
              ((symbol-function 'clutch--tx-header-line-segment) (lambda (_conn) nil))
              ((symbol-function 'clutch--header-line-indent) (lambda () "")))
      (let ((line (clutch--build-connection-header-line)))
        (should (string-match-p "default@127.0.0.1" line))
        (should (string-match-p "\\[schema\\] demo" line))))))

(ert-deftest clutch-test-disconnected-header-line-shows-backend-and-warning-state ()
  "Disconnected header line should keep backend context and warn loudly."
  (with-temp-buffer
    (setq-local clutch-connection nil
                clutch--connection-params '(:backend jdbc :driver oracle))
    ;; With nerd-icons: show icon only, no name.
    (cl-letf (((symbol-function 'clutch--connection-alive-p) (lambda (_conn) nil))
              ((symbol-function 'clutch--db-backend-icon-for-key) (lambda (_key) "[db]"))
              ((symbol-function 'clutch--nerd-icons-available-p) (lambda () t))
              ((symbol-function 'clutch--backend-display-name-from-params) (lambda (_params) "Oracle"))
              ((symbol-function 'clutch--connection-state-icon) (lambda (_connected) "[disc]"))
              ((symbol-function 'clutch--header-line-indent) (lambda () "")))
      (let* ((line (clutch--build-connection-header-line))
             (start (string-match "Disconnect" line)))
        (should (string-match-p "\\[db\\]" line))
        (should-not (string-match-p "Oracle" line))
        (should start)
        (should (eq (get-text-property start 'face line) 'warning))))
    ;; Without nerd-icons: show name, no icon.
    (cl-letf (((symbol-function 'clutch--connection-alive-p) (lambda (_conn) nil))
              ((symbol-function 'clutch--db-backend-icon-for-key) (lambda (_key) ""))
              ((symbol-function 'clutch--nerd-icons-available-p) (lambda () nil))
              ((symbol-function 'clutch--backend-display-name-from-params) (lambda (_params) "Oracle"))
              ((symbol-function 'clutch--connection-state-icon) (lambda (_connected) "[disc]"))
              ((symbol-function 'clutch--header-line-indent) (lambda () "")))
      (let* ((line (clutch--build-connection-header-line))
             (start (string-match "Disconnect" line)))
        (should (string-match-p "Oracle" line))
        (should start)
        (should (eq (get-text-property start 'face line) 'warning))))))

(ert-deftest clutch-test-connection-display-key-hides-default-port ()
  "Connection display key should omit the backend default port."
  (cl-letf (((symbol-function 'clutch-db-user) (lambda (_conn) "scott"))
            ((symbol-function 'clutch-db-host) (lambda (_conn) "dbhost"))
            ((symbol-function 'clutch-db-port) (lambda (_conn) 1521))
            ((symbol-function 'clutch-db-display-name) (lambda (_conn) "Oracle")))
    (should (equal (clutch--connection-display-key 'fake-conn)
                   "scott@dbhost"))))

(ert-deftest clutch-test-connection-display-key-keeps-nondefault-port ()
  "Connection display key should keep non-default ports."
  (cl-letf (((symbol-function 'clutch-db-user) (lambda (_conn) "scott"))
            ((symbol-function 'clutch-db-host) (lambda (_conn) "dbhost"))
            ((symbol-function 'clutch-db-port) (lambda (_conn) 1522))
            ((symbol-function 'clutch-db-display-name) (lambda (_conn) "Oracle")))
    (should (equal (clutch--connection-display-key 'fake-conn)
                   "scott@dbhost:1522"))))

(ert-deftest clutch-test-connection-display-key-omits-empty-user-prefix ()
  "Connection display keys should not show ?@ when user is absent."
  (cl-letf (((symbol-function 'clutch-db-user) (lambda (_conn) nil))
            ((symbol-function 'clutch-db-host) (lambda (_conn) "127.0.0.1"))
            ((symbol-function 'clutch-db-port) (lambda (_conn) 27017))
            ((symbol-function 'clutch-db-display-name) (lambda (_conn) "MongoDB")))
    (should (equal (clutch--connection-display-key 'fake-conn)
                   "127.0.0.1"))))

(ert-deftest clutch-test-connection-display-key-shows-sqlite-file-name ()
  "SQLite display labels should not fall back to user/host placeholders."
  (cl-letf (((symbol-function 'clutch--backend-key-from-conn)
             (lambda (_conn) 'sqlite))
            ((symbol-function 'clutch-db-database)
             (lambda (_conn) "/tmp/bookmarks.db")))
    (should (equal (clutch--connection-key 'fake-conn)
                   "sqlite:/tmp/bookmarks.db"))
    (should (equal (clutch--connection-display-key 'fake-conn)
                   "bookmarks.db"))))

(ert-deftest clutch-test-connection-display-key-shows-sqlite-memory ()
  "SQLite in-memory databases should keep their canonical label."
  (cl-letf (((symbol-function 'clutch--backend-key-from-conn)
             (lambda (_conn) 'sqlite))
            ((symbol-function 'clutch-db-database)
             (lambda (_conn) ":memory:")))
    (should (equal (clutch--connection-display-key 'fake-conn)
                   ":memory:"))))

(ert-deftest clutch-test-jdbc-backend-icon-spec-uses-database-cog-outline ()
  "Generic JDBC should use the requested database-cog-outline icon."
  (should (equal (car (car (alist-get 'jdbc clutch--db-icon-specs))) 'mdicon))
  (should (equal (cdr (car (alist-get 'jdbc clutch--db-icon-specs)))
                 "nf-md-database_cog_outline")))

(ert-deftest clutch-test-backend-candidates-affixation-omits-annotation ()
  "Backend candidates should not repeat backend names as annotations."
  (cl-letf (((symbol-function 'clutch--nerd-icons-available-p)
             (lambda () t))
            ((symbol-function 'clutch--db-backend-icon-for-key)
             (lambda (key) (format "[%s]" key))))
    (let ((row (car (clutch--backend-candidates-affixation '("sqlite")))))
      (should (equal (nth 0 row) "sqlite"))
      (should (equal (substring-no-properties (nth 1 row))
                     "[sqlite] "))
      (should (equal (nth 2 row) "")))))

(ert-deftest clutch-test-backend-candidates-affixation-marks-basic-support ()
  "Backend candidates should mark basic-support backends."
  (cl-letf (((symbol-function 'clutch--nerd-icons-available-p)
             (lambda () nil)))
    (let ((row (car (clutch--backend-candidates-affixation '("mongodb")))))
      (should (equal (nth 0 row) "mongodb"))
      (should (equal (nth 1 row) ""))
      (should (string-match-p "Basic" (substring-no-properties (nth 2 row)))))))

(ert-deftest clutch-test-mongodb-completion-uses-shell-and-collection-candidates ()
  "MongoDB completion should offer shell methods and cached collections."
  (let ((schema (make-hash-table :test 'equal)))
    (puthash "users" nil schema)
    (puthash "orders" nil schema)
    (cl-letf (((symbol-function 'clutch--schema-for-connection)
               (lambda (&optional _conn) schema)))
      (with-temp-buffer
        (clutch-mongodb-mode)
        (should (derived-mode-p 'prog-mode))
        (should-not (derived-mode-p 'js-mode))
        (should (eq indent-line-function #'clutch-mongodb-indent-line))
        (should (equal comment-start "// "))
        (insert "db.")
        (let* ((capf (clutch-mongodb-completion-at-point))
               (candidates (all-completions "" (nth 2 capf))))
          (should (equal (car capf) (point)))
          (should (equal (cadr capf) (point)))
          (should (member "users" candidates))
          (should (member "orders" candidates))
          (should (member "getCollectionNames()" candidates)))
        (erase-buffer)
        (insert "db.users.")
        (let* ((capf (clutch-mongodb-completion-at-point))
               (candidates (all-completions "" (nth 2 capf))))
          (should (member "find()" candidates))
          (should (member "aggregate()" candidates)))
        (erase-buffer)
        (insert "db.users.fi")
        (cl-letf (((symbol-function 'completion-in-region)
                   (lambda (beg end collection &optional _predicate)
                     (delete-region beg end)
                     (insert (car (all-completions
                                   "fi" collection)))
                     t)))
          (clutch-mongodb-complete-at-point)
          (should (equal (buffer-string) "db.users.find()")))))))

(ert-deftest clutch-test-mongodb-completion-offers-mql-operators-and-fields ()
  "MongoDB completion should offer MQL operators and sampled field names."
  (let ((schema (make-hash-table :test 'equal)))
    (puthash "orders" '("status" "userId" "items.productId") schema)
    (puthash "products" '("sku" "category") schema)
    (cl-letf (((symbol-function 'clutch--schema-for-connection)
               (lambda (&optional _conn) schema)))
      (with-temp-buffer
        (clutch-mongodb-mode)
        (insert "db.orders.aggregate([{$")
        (let* ((capf (clutch-mongodb-completion-at-point))
               (candidates (all-completions "$" (nth 2 capf))))
          (should (member "$match" candidates))
          (should (member "$lookup" candidates))
          (should (member "$sum" candidates))
          (should (member "$dateTrunc" candidates))
          (should (member "$vectorSearch" candidates)))
        (erase-buffer)
        (insert "db.orders.find({st")
        (let* ((capf (clutch-mongodb-completion-at-point))
               (candidates (all-completions "st" (nth 2 capf))))
          (should (member "status" candidates))
          (should-not (member "find()" candidates)))
        (erase-buffer)
        (insert "db.orders.aggregate([{$group:{_id:\"$u")
        (let* ((capf (clutch-mongodb-completion-at-point))
               (candidates (all-completions "$u" (nth 2 capf))))
          (should (member "$userId" candidates)))
        (erase-buffer)
        (insert "db.orders.aggregate([{$lookup:{from:\"pro")
        (let* ((capf (clutch-mongodb-completion-at-point))
               (candidates (all-completions "pro" (nth 2 capf))))
          (should (member "products" candidates)))
        (erase-buffer)
        (insert "db.orders.aggregate([{$merge:{into:\"pro")
        (let* ((capf (clutch-mongodb-completion-at-point))
               (candidates (all-completions "pro" (nth 2 capf))))
          (should (member "products" candidates)))))))

(ert-deftest clutch-test-mongodb-completion-inserts-key-colon ()
  "MongoDB field and operator completion should add a key separator."
  (let ((schema (make-hash-table :test 'equal)))
    (puthash "orders" '("status" "userId") schema)
    (cl-letf (((symbol-function 'clutch--schema-for-connection)
               (lambda (&optional _conn) schema)))
      (with-temp-buffer
        (clutch-mongodb-mode)
        (insert "db.orders.find({st")
        (pcase-let ((`(,beg ,end ,collection . ,plist)
                     (clutch-mongodb-completion-at-point)))
          (delete-region beg end)
          (insert "status")
          (funcall (plist-get plist :exit-function) "status" 'finished)
          (should (equal (buffer-string) "db.orders.find({status: ")))
        (erase-buffer)
        (insert "db.orders.aggregate([{$m")
        (pcase-let ((`(,beg ,end ,collection . ,plist)
                     (clutch-mongodb-completion-at-point)))
          (delete-region beg end)
          (insert "$match")
          (funcall (plist-get plist :exit-function) "$match" 'finished)
          (should (equal (buffer-string)
                         "db.orders.aggregate([{$match: ")))))))

(ert-deftest clutch-test-mongodb-mode-indents-mql-pipeline ()
  "MongoDB mode should indent MQL pipelines without deriving from JS mode."
  (with-temp-buffer
    (clutch-mongodb-mode)
    (insert "db.demo_orders.aggregate([\n{$match:{status:\"paid\"}},\n{$group:{_id:\"$userId\"}}\n])")
    (indent-region (point-min) (point-max))
    (should (equal (buffer-string)
                   "db.demo_orders.aggregate([\n  {$match:{status:\"paid\"}},\n  {$group:{_id:\"$userId\"}}\n])"))))

(ert-deftest clutch-test-mongodb-mode-fontifies-mql-structure ()
  "MongoDB mode should distinguish MQL operators, keys, methods, and strings."
  (with-temp-buffer
    (clutch-mongodb-mode)
    (insert "db.orders.aggregate([{$match:{status:\"paid\"}}])")
    (font-lock-ensure)
    (goto-char (point-min))
    (search-forward "orders")
    (should (eq (get-text-property (match-beginning 0) 'face)
                'clutch-mongodb-namespace-face))
    (search-forward "aggregate")
    (should (eq (get-text-property (match-beginning 0) 'face)
                'clutch-mongodb-method-face))
    (search-forward "$match")
    (should (eq (get-text-property (match-beginning 0) 'face)
                'clutch-mongodb-operator-face))
    (search-forward "status")
    (should (eq (get-text-property (match-beginning 0) 'face)
                'clutch-mongodb-key-face))
    (search-forward "paid")
    (should (eq (get-text-property (match-beginning 0) 'face)
                'font-lock-string-face))))

(ert-deftest clutch-test-object-browse-query-uses-mongodb-shell-syntax ()
  "MongoDB object browsing should insert a shell query, not SELECT SQL."
  (cl-letf (((symbol-function 'clutch--backend-key-from-conn)
             (lambda (_conn) 'mongodb))
            ((symbol-function 'clutch-jdbc-conn-p)
             (lambda (_conn) nil))
            ((symbol-function 'clutch--object-sql-name)
             (lambda (&rest _args)
               (error "SQL formatter should not run for MongoDB"))))
    (should (equal (clutch--object-browse-query 'mongo-conn '(:name "users"))
                   "db.getCollection(\"users\").find();"))))

(ert-deftest clutch-test-object-browse-query-uses-sql-for-mongodb-surface-sql ()
  "MongoDB SQL Interface object browsing should insert SELECT SQL."
  (cl-letf (((symbol-function 'clutch--backend-key-from-conn)
             (lambda (_conn) 'mongodb))
            ((symbol-function 'clutch-jdbc--mongodb-conn-p)
             (lambda (_conn) t))
            ((symbol-function 'clutch--object-sql-name)
             (lambda (_conn entry)
               (plist-get entry :name))))
    (should (equal (clutch--object-browse-query 'mongo-sql-conn '(:name "users"))
                   "SELECT * FROM users;"))))

(ert-deftest clutch-test-connection-state-icon-uses-database-check-outline ()
  "Connected state icon should use the requested database-check-outline glyph."
  (let (captured)
    (cl-letf (((symbol-function 'clutch--icon)
               (lambda (spec fallback)
                 (setq captured (list spec fallback))
                 "[ok]")))
      (should (equal (clutch--connection-state-icon t) "[ok]"))
      (should (equal captured '((mdicon . "nf-md-database_check_outline") "⬢"))))))

(ert-deftest clutch-test-connection-display-key-derives-generic-jdbc-endpoint ()
  "Generic JDBC connections should show endpoint details derived from :url."
  (require 'clutch-db-jdbc)
  (let ((conn (make-clutch-jdbc-conn
               :params '(:driver jdbc
                         :url "jdbc:kingbase8://127.0.0.1:54321/test"
                         :display-name "KingbaseES"
                         :user "system"))))
    (should (equal (clutch--connection-display-key conn)
                   "system@127.0.0.1:54321"))))

(ert-deftest clutch-test-jdbc-connect-uses-rpc-timeout-for-connect-rpc ()
  "JDBC connect should keep login timeout separate from RPC timeout."
  (require 'clutch-db-jdbc)
  (let ((clutch-jdbc--connections-by-id (make-hash-table :test 'eql))
        (clutch-connect-timeout-seconds 10)
        (clutch-read-idle-timeout-seconds 30)
        (clutch-query-timeout-seconds 40)
        (clutch-jdbc-rpc-timeout-seconds 50)
        captured-params
        captured-timeout)
    (cl-letf (((symbol-function 'clutch-jdbc--setup-prerequisites) #'ignore)
              ((symbol-function 'clutch-jdbc--ensure-agent) #'ignore)
              ((symbol-function 'clutch-jdbc--rpc)
               (lambda (_op params &optional timeout-seconds)
                 (setq captured-params params
                       captured-timeout timeout-seconds)
                 '(:conn-id 7))))
      (clutch-db-jdbc-connect
       'oracle
       '(:host "db.internal"
         :port 1521
         :database "ORCL"
         :user "alice"
         :connect-timeout 1
         :rpc-timeout 2))
      (should (= captured-timeout 2))
      (should (= (alist-get 'connect-timeout-seconds captured-params) 1)))))

;;;; Connection — reconnect and disconnect

(ert-deftest clutch-test-disconnect-blocks-dirty-manual-commit-connection ()
  "Disconnect should warn before dropping uncommitted manual-commit work."
  (let ((clutch--tx-dirty-cache (make-hash-table :test 'eq))
        (clutch-connection 'fake-conn)
        disconnected)
    (puthash clutch-connection t clutch--tx-dirty-cache)
    (cl-letf (((symbol-function 'clutch-db-manual-commit-p) (lambda (_conn) t))
              ((symbol-function 'clutch--connection-alive-p) (lambda (_conn) t))
              ((symbol-function 'yes-or-no-p) (lambda (_prompt) nil))
              ((symbol-function 'clutch-db-disconnect)
               (lambda (_conn) (setq disconnected t))))
      (should-error (clutch-disconnect) :type 'user-error)
      (should-not disconnected)
      (should clutch-connection)
      (should (clutch--tx-dirty-p clutch-connection)))))

(ert-deftest clutch-test-do-disconnect-stops-ssh-tunnel ()
  "Disconnect should stop any SSH tunnel associated with the connection."
  (let ((clutch--connection-remote-params-cache (make-hash-table :test 'eq))
        (clutch--connection-transport-cache (make-hash-table :test 'eq))
        disconnected
        stopped)
    (puthash 'fake-conn '(:kind ssh :process fake-proc) clutch--connection-transport-cache)
    (cl-letf (((symbol-function 'clutch--mark-dml-results-connection-closed) #'ignore)
              ((symbol-function 'clutch--invalidate-derived-buffers) #'ignore)
              ((symbol-function 'clutch--clear-tx-dirty) #'ignore)
              ((symbol-function 'clutch--forget-problem-record) #'ignore)
              ((symbol-function 'clutch-db-disconnect)
               (lambda (_conn) (setq disconnected t)))
              ((symbol-function 'process-live-p)
               (lambda (proc) (eq proc 'fake-proc)))
              ((symbol-function 'delete-process)
               (lambda (proc) (setq stopped proc))))
      (clutch--do-disconnect 'fake-conn)
      (should disconnected)
      (should (eq stopped 'fake-proc))
      (should-not (gethash 'fake-conn clutch--connection-transport-cache)))))

(ert-deftest clutch-test-kill-console-disconnects-and-invalidates ()
  "Killing a console buffer disconnects and invalidates derived buffers."
  (let ((disconnected nil)
        (console (generate-new-buffer " *clutch-console*"))
        (result (generate-new-buffer " *clutch-result*")))
    (unwind-protect
        (cl-letf (((symbol-function 'clutch--connection-alive-p) (lambda (_c) t))
                  ((symbol-function 'clutch-db-disconnect)
                   (lambda (_c) (setq disconnected t)))
                  ((symbol-function 'clutch--confirm-disconnect-transaction-loss) #'ignore)
                  ((symbol-function 'clutch--mark-dml-results-connection-closed) #'ignore)
                  ((symbol-function 'clutch--clear-tx-dirty) #'ignore)
                  ((symbol-function 'clutch--save-console) #'ignore))
          (with-current-buffer result
            (setq-local clutch-connection 'fake-conn))
          (with-current-buffer console
            (clutch-mode)
            (setq clutch-connection 'fake-conn))
          (kill-buffer console)
          (should disconnected)
          ;; Derived buffer's connection should be invalidated.
          (should-not (buffer-local-value 'clutch-connection result)))
      (when (buffer-live-p console) (kill-buffer console))
      (when (buffer-live-p result) (kill-buffer result)))))

(ert-deftest clutch-test-kill-indirect-buffer-does-not-disconnect ()
  "Killing an indirect SQL buffer must NOT disconnect the connection."
  (let ((disconnected nil)
        (buf (generate-new-buffer " *clutch-indirect*")))
    (unwind-protect
        (cl-letf (((symbol-function 'clutch--connection-alive-p) (lambda (_c) t))
                  ((symbol-function 'clutch-db-disconnect)
                   (lambda (_c) (setq disconnected t)))
                  ((symbol-function 'clutch--save-console) #'ignore))
          (with-current-buffer buf
            (clutch-mode)
            (clutch--indirect-mode 1)
            (setq clutch-connection 'fake-conn))
          (kill-buffer buf))
      (when (buffer-live-p buf) (kill-buffer buf)))
    (should-not disconnected)))

(ert-deftest clutch-test-kill-plain-sql-buffer-disconnects ()
  "Killing a plain `clutch-mode' buffer with no console-name should still disconnect.
This applies when the buffer owns the connection."
  (let ((disconnected nil)
        (buf (generate-new-buffer " *clutch-plain-sql*")))
    (unwind-protect
        (cl-letf (((symbol-function 'clutch--connection-alive-p) (lambda (_c) t))
                  ((symbol-function 'clutch-db-disconnect)
                   (lambda (_c) (setq disconnected t)))
                  ((symbol-function 'clutch--confirm-disconnect-transaction-loss) #'ignore)
                  ((symbol-function 'clutch--mark-dml-results-connection-closed) #'ignore)
                  ((symbol-function 'clutch--clear-tx-dirty) #'ignore)
                  ((symbol-function 'clutch--save-console) #'ignore))
          (with-current-buffer buf
            (clutch-mode)
            ;; No clutch--console-name, no clutch--indirect-mode.
            (setq clutch-connection 'fake-conn))
          (kill-buffer buf))
      (when (buffer-live-p buf) (kill-buffer buf)))
    (should disconnected)))

(ert-deftest clutch-test-kill-repl-buffer-disconnects ()
  "Killing a REPL buffer that owns a connection should disconnect."
  (let ((disconnected nil)
        (buf (generate-new-buffer " *clutch-repl-kill*")))
    (unwind-protect
        (cl-letf (((symbol-function 'clutch--connection-alive-p) (lambda (_c) t))
                  ((symbol-function 'clutch-db-disconnect)
                   (lambda (_c) (setq disconnected t)))
                  ((symbol-function 'clutch--confirm-disconnect-transaction-loss) #'ignore)
                  ((symbol-function 'clutch--mark-dml-results-connection-closed) #'ignore)
                  ((symbol-function 'clutch--clear-tx-dirty) #'ignore))
          (with-current-buffer buf
            (clutch-repl-mode)
            (setq clutch-connection 'fake-conn))
          (kill-buffer buf))
      (when (buffer-live-p buf) (kill-buffer buf)))
    (should disconnected)))

(ert-deftest clutch-test-reconnect-invalidates-derived-buffers ()
  "Reconnecting in a console should invalidate derived buffers holding the old connection."
  (let ((old-conn (list 'old))
        (new-conn (list 'new))
        (result (generate-new-buffer " *clutch-result-reconnect*")))
    (unwind-protect
        (cl-letf (((symbol-function 'clutch--connection-alive-p) (lambda (c) (eq c old-conn)))
                  ((symbol-function 'clutch-db-disconnect) #'ignore)
                  ((symbol-function 'clutch--confirm-disconnect-transaction-loss) #'ignore)
                  ((symbol-function 'clutch--mark-dml-results-connection-closed) #'ignore)
                  ((symbol-function 'clutch--clear-tx-dirty) #'ignore)
                  ((symbol-function 'clutch--read-connection-params)
                   (lambda () '(:backend mysql :host "localhost")))
                  ((symbol-function 'clutch--effective-sql-product) (lambda (_p) 'mysql))
                  ((symbol-function 'clutch--build-conn) (lambda (_p) new-conn))
                  ((symbol-function 'clutch--bind-connection-context) #'ignore)
                  ((symbol-function 'clutch--prime-schema-cache) #'ignore)
                  ((symbol-function 'clutch--update-mode-line) #'ignore)
                  ((symbol-function 'clutch--connection-key) (lambda (_c) "test")))
          (with-current-buffer result
            (setq-local clutch-connection old-conn))
          (let ((clutch-connection old-conn))
            (clutch-connect))
          ;; After reconnect, derived buffer should be invalidated.
          (should-not (buffer-local-value 'clutch-connection result)))
      (when (buffer-live-p result) (kill-buffer result)))))

(ert-deftest clutch-test-kill-result-buffer-does-not-disconnect ()
  "Killing a derived result buffer must NOT disconnect the connection."
  (let ((disconnected nil)
        (buf (generate-new-buffer " *clutch-result-kill*")))
    (unwind-protect
        (cl-letf (((symbol-function 'clutch-db-disconnect)
                   (lambda (_c) (setq disconnected t)))
                  ((symbol-function 'clutch--result-buffer-cleanup) #'ignore))
          (with-current-buffer buf
            (clutch-result-mode)
            (setq clutch-connection 'fake-conn))
          (kill-buffer buf))
      (when (buffer-live-p buf) (kill-buffer buf)))
    (should-not disconnected)))

(ert-deftest clutch-test-reconnect-preserves-pending ()
  "Reconnect preserves staged changes in result buffers."
  (let ((result-buf (generate-new-buffer "*clutch-test-result*"))
        (clutch-buf (generate-new-buffer "*clutch-test*")))
    (unwind-protect
        (progn
          (with-current-buffer result-buf
            (clutch-result-mode)
            (setq-local clutch--pending-deletes (list (vector 1)))
            (setq-local clutch--pending-edits nil)
            (setq-local clutch--pending-inserts nil))
          (with-current-buffer clutch-buf
            (cl-letf (((symbol-function 'clutch--build-conn)
                       (lambda (_) 'fake-conn))
                      ((symbol-function 'clutch-db-live-p)
                       (lambda (_) t))
                      ((symbol-function 'clutch--connection-key)
                       (lambda (_) "fake"))
                      ((symbol-function 'clutch--update-mode-line) #'ignore))
              (setq-local clutch--connection-params '(:backend mysql))
              (clutch--try-reconnect)))
          (with-current-buffer result-buf
            (should (equal clutch--pending-deletes (list (vector 1))))))
      (kill-buffer result-buf)
      (kill-buffer clutch-buf))))

(ert-deftest clutch-test-try-reconnect-releases-old-ssh-transport ()
  "Reconnect should stop the old SSH tunnel after the new connection is ready."
  (let ((released nil))
    (with-temp-buffer
      (setq-local clutch-connection 'old-conn
                  clutch--connection-params '(:backend pg
                                              :host "db.internal"
                                              :port 5432
                                              :ssh-host "bastion-prod")
                  clutch--conn-sql-product 'pg)
      (cl-letf (((symbol-function 'clutch--build-conn)
                 (lambda (_params) 'new-conn))
                ((symbol-function 'clutch--clear-tx-dirty) #'ignore)
                ((symbol-function 'clutch--release-connection-transport)
                 (lambda (conn) (setq released conn)))
                ((symbol-function 'clutch--rebind-connection-buffers) #'ignore)
                ((symbol-function 'clutch--finalize-rebound-connection)
                 (lambda (_conn) 'done))
                ((symbol-function 'clutch--connection-key)
                 (lambda (_conn) "alice@db.internal:5432/appdb"))
                ((symbol-function 'message) #'ignore))
        (should (clutch--try-reconnect))
        (should (eq released 'old-conn))))))

(ert-deftest clutch-test-result-buffer-reconnects-using-inherited-context ()
  "Result buffers should inherit reconnect params from their source buffer."
  (let (result-buf built)
    (unwind-protect
        (with-temp-buffer
          (let ((source-buf (current-buffer))
                (result (make-clutch-db-result
                         :connection 'old-conn
                         :columns nil
                         :rows nil)))
            (setq-local clutch-connection 'old-conn
                        clutch--connection-params '(:backend mysql :database "db")
                        clutch--conn-sql-product 'mysql)
            (cl-letf (((symbol-function 'clutch-db-live-p)
                       (lambda (_conn) t))
                      ((symbol-function 'clutch--connection-key)
                       (lambda (conn) (symbol-name conn)))
                      ((symbol-function 'clutch-result--display-dml) #'ignore)
                      ((symbol-function 'clutch-result--show-buffer)
                       (lambda (buf) (setq result-buf buf) buf)))
              (clutch-result--display result "UPDATE demo SET x = 1" 0.1))
            (with-current-buffer result-buf
              (should (equal clutch--connection-params
                             '(:backend mysql :database "db")))
              (should (eq clutch--conn-sql-product 'mysql))
              (cl-letf (((symbol-function 'clutch-db-live-p)
                         (lambda (conn) (eq conn 'new-conn)))
                        ((symbol-function 'clutch--build-conn)
                         (lambda (params)
                           (setq built params)
                           'new-conn))
                        ((symbol-function 'clutch--clear-tx-dirty) #'ignore)
                        ((symbol-function 'clutch--prime-schema-cache) #'ignore)
                        ((symbol-function 'clutch--refresh-schema-status-ui) #'ignore)
                        ((symbol-function 'clutch--refresh-transaction-ui) #'ignore)
                        ((symbol-function 'clutch--refresh-result-status-line) #'ignore)
                        ((symbol-function 'clutch--connection-key)
                         (lambda (conn) (symbol-name conn)))
                        ((symbol-function 'message) #'ignore))
                (clutch--ensure-connection)
                (should (eq clutch-connection 'new-conn))
                (should (equal built '(:backend mysql :database "db")))))
            (with-current-buffer source-buf
              (should (eq clutch-connection 'new-conn)))))
      (when (buffer-live-p result-buf)
        (kill-buffer result-buf)))))

(ert-deftest clutch-test-object-buffer-reconnects-using-inherited-context ()
  "Object definition buffers should inherit reconnect params from their source buffer."
  (let (object-buf built)
    (unwind-protect
        (with-temp-buffer
          (let ((source-buf (current-buffer)))
            (setq-local clutch-connection 'old-conn
                        clutch--connection-params '(:backend mysql :database "db")
                        clutch--conn-sql-product 'mysql)
            (cl-letf (((symbol-function 'clutch-db-show-create-table)
                       (lambda (_conn _table) "CREATE TABLE demo (id INT)"))
                      ((symbol-function 'sql-mode) #'ignore)
                      ((symbol-function 'sql-set-product) #'ignore)
                      ((symbol-function 'font-lock-ensure) #'ignore)
                      ((symbol-function 'clutch--connection-key)
                       (lambda (conn) (symbol-name conn)))
                      ((symbol-function 'pop-to-buffer)
                       (lambda (buf &rest _args)
                         (setq object-buf buf)
                         buf)))
              (clutch-object-show-ddl-or-source '(:name "demo" :type "TABLE")))
            (with-current-buffer object-buf
              (should (equal clutch--connection-params
                             '(:backend mysql :database "db")))
              (should (eq clutch--conn-sql-product 'mysql))
              (cl-letf (((symbol-function 'clutch-db-live-p)
                         (lambda (conn) (eq conn 'new-conn)))
                        ((symbol-function 'clutch--build-conn)
                         (lambda (params)
                           (setq built params)
                           'new-conn))
                        ((symbol-function 'clutch--clear-tx-dirty) #'ignore)
                        ((symbol-function 'clutch--prime-schema-cache) #'ignore)
                        ((symbol-function 'clutch--refresh-schema-status-ui) #'ignore)
                        ((symbol-function 'clutch--refresh-transaction-ui) #'ignore)
                        ((symbol-function 'clutch--connection-key)
                         (lambda (conn) (symbol-name conn)))
                        ((symbol-function 'message) #'ignore))
                (clutch--ensure-connection)
                (should (eq clutch-connection 'new-conn))
                (should (equal built '(:backend mysql :database "db")))))
            (with-current-buffer source-buf
              (should (eq clutch-connection 'new-conn)))))
      (when (buffer-live-p object-buf)
        (kill-buffer object-buf)))))

(ert-deftest clutch-test-connect-failure-preserves-old-live-connection ()
  "Interactive connect should not drop the old live session on failure."
  (let ((clutch-connection 'old-conn)
        disconnected)
    (with-temp-buffer
      (setq-local clutch-connection 'old-conn)
      (cl-letf (((symbol-function 'clutch--connection-alive-p)
                 (lambda (_conn) t))
                ((symbol-function 'clutch--confirm-disconnect-transaction-loss)
                 #'ignore)
                ((symbol-function 'clutch--read-connection-params)
                 (lambda () '(:backend mysql :database "newdb")))
                ((symbol-function 'clutch--build-conn)
                 (lambda (_params)
                   (signal 'clutch-db-error '("connect failed"))))
                ((symbol-function 'clutch-db-disconnect)
                 (lambda (_conn) (setq disconnected t))))
        (should-error (clutch-connect) :type 'clutch-db-error)
        (should (eq clutch-connection 'old-conn))
        (should-not disconnected)))))

(ert-deftest clutch-test-connect-rebuilds-conn-when-agent-dies-during-disconnect ()
  "Reconnect should rebuild a dead new connection after old disconnect."
  (let ((built nil)
        (activated nil))
    (with-temp-buffer
      (clutch-mode)
      (setq-local clutch-connection 'old-conn)
      (cl-letf (((symbol-function 'clutch--connection-alive-p)
                 (lambda (conn)
                   (pcase conn
                     ('old-conn t)
                     ('new-conn-1 nil)
                     (_ t))))
                ((symbol-function 'clutch--confirm-disconnect-transaction-loss)
                 #'ignore)
                ((symbol-function 'clutch--connect-params-for-current-buffer)
                 (lambda () '(:backend jdbc :database "newdb")))
                ((symbol-function 'clutch--effective-sql-product)
                 (lambda (_params) 'jdbc))
                ((symbol-function 'clutch--build-conn)
                 (lambda (params)
                   (push params built)
                   (pcase (length built)
                     (1 'new-conn-1)
                     (2 'new-conn-2)
                     (_ 'unexpected-conn))))
                ((symbol-function 'clutch--do-disconnect)
                 #'ignore)
                ((symbol-function 'clutch--activate-current-buffer-connection)
                 (lambda (conn params product)
                   (setq activated (list conn params product))))
                ((symbol-function 'clutch--connection-key)
                 (lambda (_conn) "test-conn"))
                ((symbol-function 'message) #'ignore))
        (clutch-connect)
        (should (= (length built) 2))
        (should (equal (nreverse built)
                       '((:backend jdbc :database "newdb")
                         (:backend jdbc :database "newdb"))))
        (should (equal activated
                       (list 'new-conn-2
                             (clutch--materialize-connection-params
                              '(:backend jdbc :database "newdb"))
                             'jdbc)))))))

(ert-deftest clutch-test-connect-clears-stale-schema-refresh-state-before-prime ()
  "Reconnect should not inherit a stale `refreshing' schema status."
  (let ((clutch--schema-cache (make-hash-table :test 'equal))
        (clutch--column-details-cache (make-hash-table :test 'equal))
        (clutch--column-details-status-cache (make-hash-table :test 'equal))
        (clutch--column-details-queue-cache (make-hash-table :test 'equal))
        (clutch--column-details-active-cache (make-hash-table :test 'equal))
        (clutch--columns-status-cache (make-hash-table :test 'equal))
        (clutch--table-comment-cache (make-hash-table :test 'equal))
        (clutch--table-comment-status-cache (make-hash-table :test 'equal))
        (clutch--help-doc-cache (make-hash-table :test 'equal))
        (clutch--schema-install-timers (make-hash-table :test 'equal))
        (clutch--schema-status-cache (make-hash-table :test 'equal))
        (clutch--schema-refresh-tickets (make-hash-table :test 'equal))
        (clutch--object-cache (make-hash-table :test 'equal))
        (clutch--object-warmup-timers (make-hash-table :test 'equal))
        (clutch--object-warmup-generations (make-hash-table :test 'equal))
        status-before-prime)
    (puthash "same-key" '(:state refreshing) clutch--schema-status-cache)
    (puthash "same-key" 7 clutch--schema-refresh-tickets)
    (with-temp-buffer
      (setq-local clutch-connection 'old-conn
                  clutch--console-name "dev")
      (cl-letf (((symbol-function 'clutch--connection-alive-p)
                 (lambda (conn) (eq conn 'new-conn)))
                ((symbol-function 'clutch--connect-params-for-current-buffer)
                 (lambda () '(:backend mysql :database "app")))
                ((symbol-function 'clutch--materialize-connection-params)
                 #'identity)
                ((symbol-function 'clutch--effective-sql-product)
                 (lambda (_params) 'mysql))
                ((symbol-function 'clutch--build-conn)
                 (lambda (_params) 'new-conn))
                ((symbol-function 'clutch--connection-key)
                 (lambda (_conn) "same-key"))
                ((symbol-function 'clutch--prime-schema-cache)
                 (lambda (_conn)
                   (setq status-before-prime
                         (gethash "same-key" clutch--schema-status-cache))))
                ((symbol-function 'clutch--update-mode-line) #'ignore)
                ((symbol-function 'message) #'ignore))
        (clutch-connect)
        (should (eq clutch-connection 'new-conn))
        (should-not status-before-prime)))))

(ert-deftest clutch-test-replace-connection-rebuilds-dead-conn-after-disconnect ()
  "Replacing a connection should rebuild if the first new conn dies during disconnect."
  (let (built rebound finalized disconnected cleared)
    (cl-letf (((symbol-function 'clutch--effective-sql-product)
               (lambda (_params) 'clickhouse))
              ((symbol-function 'clutch--connection-key)
               (lambda (_conn) "default-key"))
              ((symbol-function 'clutch--build-conn)
               (lambda (params)
                 (push params built)
                 (pcase (length built)
                   (1 'new-conn-1)
                   (2 'new-conn-2)
                   (_ 'unexpected-conn))))
              ((symbol-function 'clutch--clear-tx-dirty)
               (lambda (_conn) nil))
              ((symbol-function 'clutch--connection-alive-p)
               (lambda (conn)
                 (pcase conn
                   ('old-conn t)
                   ('new-conn-1 nil)
                   ('new-conn-2 t)
                   (_ nil))))
              ((symbol-function 'clutch-db-disconnect)
               (lambda (conn) (setq disconnected conn)))
              ((symbol-function 'clutch--rebind-connection-buffers)
               (lambda (_old new params product)
                 (setq rebound (list new params product))))
              ((symbol-function 'clutch--clear-connection-metadata-caches)
               (lambda (conn &optional key)
                 (push (list conn key) cleared)))
              ((symbol-function 'clutch--finalize-rebound-connection)
               (lambda (conn) (setq finalized conn) conn)))
      (clutch--replace-connection 'old-conn '(:backend clickhouse :database "demo") 'clickhouse)
      (should (equal (nreverse built)
                     '((:backend clickhouse :database "demo")
                       (:backend clickhouse :database "demo"))))
      (should (eq disconnected 'old-conn))
      (should (equal rebound '(new-conn-2 (:backend clickhouse :database "demo") clickhouse)))
      (should (eq finalized 'new-conn-2))
      (should (equal (nreverse cleared)
                     '((old-conn "default-key")
                       (new-conn-2 nil)))))))

(ert-deftest clutch-test-connect-in-query-console-reuses-saved-connection ()
  "Query console reconnect should reuse that console's saved connection."
  (with-temp-buffer
    (let ((clutch-connection-alist
           '(("alpha" . (:backend mysql :database "app_a"))
             ("beta" . (:backend mysql :database "app_b"))))
          built
          read-called)
      (setq-local clutch--console-name "alpha")
      (cl-letf (((symbol-function 'clutch--connection-alive-p)
                 (lambda (_conn) nil))
                ((symbol-function 'clutch--read-connection-params)
                 (lambda ()
                   (setq read-called t)
                   '(:backend mysql :database "should-not-be-used")))
                ((symbol-function 'clutch--effective-sql-product)
                 (lambda (_params) 'mysql))
                ((symbol-function 'clutch--build-conn)
                 (lambda (params)
                   (setq built params)
                   'new-conn))
                ((symbol-function 'clutch--connection-key)
                 (lambda (_conn) "test-conn"))
                ((symbol-function 'clutch--activate-current-buffer-connection)
                 (lambda (_conn _params _product) nil))
                ((symbol-function 'message) #'ignore))
        (clutch-connect)
        (should-not read-called)
        (should (equal built '(:backend mysql :database "app_a" :pass-entry "alpha")))))))

(ert-deftest clutch-test-connect-in-query-console-preserves-tramp-origin ()
  "Query console reconnect should keep its stored TRAMP origin."
  (with-temp-buffer
    (let ((clutch-connection-alist
           '(("alpha" . (:backend pg
                         :host "db"
                         :port 5432
                         :database "appdb"))))
          (clutch-tramp-context-policy 'ask)
          built)
      (setq-local clutch--console-name "alpha"
                  clutch--connection-params
                  '(:backend pg
                    :host "db"
                    :port 5432
                    :database "appdb"
                    :pass-entry "alpha"
                    :tramp-default-directory "/ssh:devbox:/workspace/"))
      (cl-letf (((symbol-function 'clutch--connection-alive-p)
                 (lambda (_conn) nil))
                ((symbol-function 'y-or-n-p)
                 (lambda (_prompt)
                   (ert-fail "stored TRAMP origin should not prompt again")))
                ((symbol-function 'clutch--effective-sql-product)
                 (lambda (_params) 'postgres))
                ((symbol-function 'clutch--build-conn)
                 (lambda (params)
                   (setq built params)
                   'new-conn))
                ((symbol-function 'clutch--connection-key)
                 (lambda (_conn) "test-conn"))
                ((symbol-function 'clutch--activate-current-buffer-connection)
                 (lambda (_conn _params _product) nil))
                ((symbol-function 'message) #'ignore))
        (clutch-connect)
        (should (equal built
                       '(:backend pg
                         :host "db"
                         :port 5432
                         :database "appdb"
                         :pass-entry "alpha"
                         :tramp-default-directory "/ssh:devbox:/workspace/")))))))

(ert-deftest clutch-test-connect-in-query-console-skips-stale-tramp-origin-for-unforwardable-profile ()
  "Reconnect should not carry old TRAMP origin onto non-TCP profiles."
  (dolist (case '((:saved (:backend sqlite :database "/tmp/app.db")
                   :expected (:backend sqlite :database "/tmp/app.db"
                              :pass-entry "alpha"))
                  (:saved (:backend oracle
                           :url "jdbc:oracle:thin:@//db.example.com:1521/ORCL")
                   :expected (:backend oracle
                              :url "jdbc:oracle:thin:@//db.example.com:1521/ORCL"
                              :pass-entry "alpha"))))
    (let* ((saved (plist-get case :saved))
           (expected (plist-get case :expected)))
      (let ((clutch-connection-alist `(("alpha" . ,saved)))
            (clutch-tramp-context-policy 'ask)
            built)
        (with-temp-buffer
          (setq-local clutch--console-name "alpha"
                      clutch--connection-params
                      '(:backend pg
                        :host "db"
                        :port 5432
                        :database "appdb"
                        :pass-entry "alpha"
                        :tramp-default-directory "/ssh:devbox:/workspace/"))
          (cl-letf (((symbol-function 'clutch--connection-alive-p)
                     (lambda (_conn) nil))
                    ((symbol-function 'y-or-n-p)
                     (lambda (_prompt)
                       (ert-fail "unforwardable profile should not prompt for TRAMP")))
                    ((symbol-function 'clutch--resolve-password)
                     (lambda (params)
                       (when (clutch--jdbc-backend-p (plist-get params :backend))
                         "secret")))
                    ((symbol-function 'clutch--effective-sql-product)
                     (lambda (_params) 'generic))
                    ((symbol-function 'clutch--build-conn)
                     (lambda (params)
                       (setq built params)
                       'new-conn))
                    ((symbol-function 'clutch--connection-key)
                     (lambda (_conn) "test-conn"))
                    ((symbol-function 'clutch--activate-current-buffer-connection)
                     (lambda (_conn _params _product) nil))
                    ((symbol-function 'message) #'ignore))
            (clutch-connect)
            (should (equal built expected))))))))

(ert-deftest clutch-test-connect-stores-resolved-password-in-connection-context ()
  "Connect should retain resolved credentials for reconnects."
  (with-temp-buffer
    (let ((clutch-connection-alist
           '(("alpha" . (:backend mysql :database "app_a"))))
          built
          activated)
      (setq-local clutch--console-name "alpha")
      (cl-letf (((symbol-function 'clutch--connection-alive-p)
                 (lambda (_conn) nil))
                ((symbol-function 'clutch--resolve-password)
                 (lambda (_params) "secret"))
                ((symbol-function 'clutch--effective-sql-product)
                 (lambda (_params) 'mysql))
                ((symbol-function 'clutch--build-conn)
                 (lambda (params)
                   (setq built params)
                   'new-conn))
                ((symbol-function 'clutch--connection-key)
                 (lambda (_conn) "test-conn"))
                ((symbol-function 'clutch--activate-current-buffer-connection)
                 (lambda (_conn params _product)
                   (setq activated params)
                   nil))
                ((symbol-function 'message) #'ignore))
        (clutch-connect)
        (should (equal built
                       '(:backend mysql :database "app_a"
                         :pass-entry "alpha")))
        (should (equal activated
                       (clutch--materialize-connection-params
                        '(:backend mysql :database "app_a"
                          :pass-entry "alpha"))))
        ))))

(ert-deftest clutch-test-connect-in-query-console-errors-when-saved-connection-missing ()
  "Query console reconnect should error when its saved connection no longer exists."
  (with-temp-buffer
    (let ((clutch-connection-alist '(("beta" . (:backend mysql :database "app_b"))))
          read-called)
      (setq-local clutch--console-name "alpha")
      (cl-letf (((symbol-function 'clutch--connection-alive-p)
                 (lambda (_conn) nil))
                ((symbol-function 'clutch--read-connection-params)
                 (lambda ()
                   (setq read-called t)
                   '(:backend mysql :database "fallback"))))
        (should-error (clutch-connect) :type 'user-error)
        (should-not read-called)))))

(ert-deftest clutch-test-query-console-does-not-create-buffer-on-connect-failure ()
  "Query console should not create a visible buffer before connect succeeds."
  (let* ((name "alpha")
         (buffer-name (clutch--console-buffer-base-name name))
         (clutch-connection-alist
          '(("alpha" . (:backend mysql :database "app_a")))))
    (unwind-protect
        (cl-letf (((symbol-function 'clutch--effective-sql-product)
                   (lambda (_params) 'mysql))
                  ((symbol-function 'clutch--build-conn)
                   (lambda (_params)
                     (user-error "Connection refused"))))
          (should-error (clutch-query-console name) :type 'user-error)
          (should-not (get-buffer buffer-name)))
      (when-let* ((buf (get-buffer buffer-name)))
        (kill-buffer buf)))))

(defmacro clutch-test--with-query-console-build-stubs (spec &rest body)
  "Run BODY with query-console connection construction stubbed."
  (declare (indent 1))
  (pcase-let ((`(,built ,product ,conn) spec))
    `(cl-letf (((symbol-function 'clutch--build-conn)
                (lambda (conn-params)
                  (setq ,built conn-params)
                  ,conn))
               ((symbol-function 'clutch--effective-sql-product)
                (lambda (_params) ,product))
               ((symbol-function 'clutch--activate-current-buffer-connection)
                (lambda (conn conn-params product)
                  (setq-local clutch-connection conn
                              clutch--connection-params conn-params
                              clutch--conn-sql-product product)))
               ((symbol-function 'clutch--update-console-buffer-name)
                #'ignore))
       ,@body)))

(ert-deftest clutch-test-query-console-opens-ad-hoc-sqlite-file ()
  "Query console should open a SQL workspace for an ad hoc SQLite file."
  (let* ((db-file "/tmp/clutch-direct-console.db")
         (name (format "SQLite: %s" (abbreviate-file-name db-file)))
         (buffer-name (clutch--console-buffer-base-name name))
         (params (list :backend 'sqlite :database db-file))
         built)
    (unwind-protect
        (cl-letf (((symbol-function 'read-file-name)
                   (lambda (&rest _args) db-file)))
          (clutch-test--with-query-console-build-stubs (built 'sqlite 'sqlite-conn)
            (clutch-query-sqlite-file db-file)
            (should (equal built params))
            (should (eq clutch-connection 'sqlite-conn))
            (should (equal clutch--connection-params params))
            (should (eq clutch--conn-sql-product 'sqlite))
            (should (equal clutch--console-name name))
            (should (equal clutch--console-ad-hoc-params params))
            (should (eq (current-buffer) (get-buffer buffer-name)))))
      (when-let* ((buf (get-buffer buffer-name)))
        (kill-buffer buf)))))

(ert-deftest clutch-test-query-console-mongodb-uses-mongodb-mode ()
  "MongoDB query consoles should use MQL editing, not SQL or JS mode."
  (let* ((name "mongo-local")
         (buffer-name (clutch--console-buffer-base-name name))
         (params '(:backend mongodb
                   :host "127.0.0.1"
                   :port 27017
                   :database "app"))
         built)
    (unwind-protect
        (clutch-test--with-query-console-build-stubs (built nil 'mongodb-conn)
          (clutch-query-console (list :name name :params params))
          (should (equal built params))
          (should (eq major-mode 'clutch-mongodb-mode))
          (should (derived-mode-p 'prog-mode))
          (should-not (derived-mode-p 'js-mode))
          (should-not (derived-mode-p 'sql-mode))
          (should (eq indent-line-function #'clutch-mongodb-indent-line))
          (should (memq #'clutch-mongodb-completion-at-point
                        completion-at-point-functions))
          (should-not (memq #'clutch-completion-at-point
                            completion-at-point-functions)))
      (when-let* ((buf (get-buffer buffer-name)))
        (kill-buffer buf)))))

(ert-deftest clutch-test-query-console-mongodb-surface-sql-uses-sql-mode ()
  "MongoDB SQL Interface consoles should use SQL editing under the MongoDB backend."
  (let* ((name "mongo-sql-local")
         (buffer-name (clutch--console-buffer-base-name name))
         (params '(:backend mongodb
                   :surface sql-interface
                   :host "cluster0.a.query.mongodb.net"
                   :database "analytics"))
         built)
    (unwind-protect
        (clutch-test--with-query-console-build-stubs (built nil 'mongodb-conn)
          (clutch-query-console (list :name name :params params))
          (should (equal built params))
          (should (eq major-mode 'clutch-mode))
          (should (derived-mode-p 'sql-mode))
          (should-not (derived-mode-p 'js-mode))
          (should (memq #'clutch-completion-at-point
                        completion-at-point-functions))
          (should-not (memq #'clutch-mongodb-completion-at-point
                            completion-at-point-functions)))
      (when-let* ((buf (get-buffer buffer-name)))
        (kill-buffer buf)))))

(ert-deftest clutch-test-query-console-no-match-opens-sqlite-file ()
  "No-match query-console choice should open the new-connection flow."
  (let* ((db-file "/tmp/clutch-new-sqlite-console.db")
         (name (format "SQLite: %s" (abbreviate-file-name db-file)))
         (buffer-name (clutch--console-buffer-base-name name))
         (params (list :backend 'sqlite :database db-file))
         (clutch-connection-alist
          '(("alpha" . (:backend mysql :database "app_a"))))
         console-candidates
         built)
    (unwind-protect
        (cl-letf (((symbol-function 'completing-read)
                   (lambda (prompt collection &rest _args)
                     (pcase prompt
                       ("Console: "
                        (setq console-candidates collection)
                        "")
                       ("Backend: " "sqlite")
                       (_ (error "Unexpected prompt: %s" prompt)))))
                  ((symbol-function 'read-file-name)
                   (lambda (&rest _args) db-file)))
          (clutch-test--with-query-console-build-stubs (built 'sqlite 'sqlite-conn)
            (call-interactively #'clutch-query-console)
            (should (member "alpha" console-candidates))
            (should-not (member "New connection..." console-candidates))
            (should-not (member "SQLite file..." console-candidates))
            (should (equal built params))
            (should (equal clutch--console-ad-hoc-params params))
            (should (eq (current-buffer) (get-buffer buffer-name)))))
      (when-let* ((buf (get-buffer buffer-name)))
        (kill-buffer buf)))))

(ert-deftest clutch-test-query-console-choice-affixation ()
  "Saved query-console candidates should expose backend metadata as affixes."
  (dolist (case '((t "[sqlite] ")
                  (nil "")))
    (pcase-let ((`(,icons-p ,expected-prefix) case))
      (let ((clutch-connection-alist
             '(("alpha" . (:backend sqlite :database "/tmp/bookmarks.db"))))
            affixation)
        (cl-letf (((symbol-function 'clutch--nerd-icons-available-p)
                   (lambda () icons-p))
                  ((symbol-function 'clutch--db-backend-icon-for-key)
                   (lambda (_key) "[sqlite]"))
                  ((symbol-function 'completing-read)
                   (lambda (_prompt collection &rest _args)
                     (should (equal collection '("alpha")))
                     (setq affixation
                           (plist-get completion-extra-properties
                                      :affixation-function))
                     "alpha")))
          (should (equal (clutch--read-query-console-choice '("alpha"))
                         "alpha"))
          (let ((row (car (funcall affixation '("alpha")))))
            (should (equal (nth 0 row) "alpha"))
            (should (equal (substring-no-properties (nth 1 row))
                           expected-prefix))
            (should (equal (substring-no-properties (nth 2 row))
                           "  /tmp/bookmarks.db"))))))))

(ert-deftest clutch-test-query-console-no-match-reads-network-params ()
  "No-match query-console choice should collect temporary network params."
  (let* ((params '(:backend pg
                   :host "db.example.com"
                   :port 5544
                   :user "alice"
                   :ssh-host "bastion-prod"
                   :password "secret"
                   :database "app_db"))
         (name (clutch--ad-hoc-console-name params))
         (buffer-name (clutch--console-buffer-base-name name))
         (clutch-connection-alist
          '(("alpha" . (:backend mysql :database "app_a"))))
         built
         port-default)
    (unwind-protect
        (cl-letf (((symbol-function 'completing-read)
                   (lambda (prompt _collection &rest _args)
                     (pcase prompt
                       ("Console: " "new-pg")
                       ("Backend: " "pg")
                       (_ (error "Unexpected prompt: %s" prompt)))))
                  ((symbol-function 'read-string)
                   (lambda (prompt &optional _initial _history default-value _inherit)
                     (pcase prompt
                       ("Host (127.0.0.1): " "db.example.com")
                       ("User: " "alice")
                       ("SSH host from ~/.ssh/config (optional): " "bastion-prod")
                       ("Database (optional): " "app_db")
                       (_ (or default-value "")))))
                  ((symbol-function 'read-number)
                   (lambda (_prompt default)
                     (setq port-default default)
                     5544))
                  ((symbol-function 'clutch--resolve-password)
                   (lambda (_params) nil))
                  ((symbol-function 'read-passwd)
                   (lambda (&rest _args) "secret")))
          (clutch-test--with-query-console-build-stubs (built 'pg 'pg-conn)
            (call-interactively #'clutch-query-console)
            (should (= port-default 5432))
            (should (equal built params))
            (should (equal clutch--console-ad-hoc-params params))
            (should (eq (current-buffer) (get-buffer buffer-name)))))
      (when-let* ((buf (get-buffer buffer-name)))
        (kill-buffer buf)))))

(ert-deftest clutch-test-query-console-infers-tramp-origin-from-source-buffer ()
  "Query console connection origin should use the command source buffer."
  (let* ((name "alpha")
         (buffer-name (clutch--console-buffer-base-name name))
         (clutch-connection-alist
          '(("alpha" . (:backend pg
                        :host "db"
                        :port 5432
                        :user "alice"
                        :database "appdb"))))
         (clutch-tramp-context-policy 'auto)
         (default-directory "/ssh:devbox:/workspace/")
         built)
    (unwind-protect
        (clutch-test--with-query-console-build-stubs (built 'postgres 'pg-conn)
          (clutch-query-console name)
          (should (equal built
                         '(:backend pg
                           :host "db"
                           :port 5432
                           :user "alice"
                           :database "appdb"
                           :pass-entry "alpha"
                           :tramp-default-directory "/ssh:devbox:/workspace/")))
          (should (equal clutch--connection-params built))
          (should (eq (current-buffer) (get-buffer buffer-name))))
      (when-let* ((buf (get-buffer buffer-name)))
        (kill-buffer buf)))))

(ert-deftest clutch-test-query-console-infers-container-tramp-origin-from-source-buffer ()
  "Query console should infer supported container TRAMP origins."
  (let* ((name "alpha")
         (buffer-name (clutch--console-buffer-base-name name))
         (clutch-connection-alist
          '(("alpha" . (:backend pg
                        :host "127.0.0.1"
                        :port 5432
                        :user "alice"
                        :database "appdb"))))
         (clutch-tramp-context-policy 'ask)
         (default-directory "/docker:vscode@f500f94f96e3:/workspace/")
         built)
    (unwind-protect
        (cl-letf (((symbol-function 'y-or-n-p)
                   (lambda (prompt)
                     (should (string-match-p "/docker:vscode@f500f94f96e3:" prompt))
                     t)))
          (clutch-test--with-query-console-build-stubs (built 'postgres 'pg-conn)
            (clutch-query-console name)
            (should (equal built
                           '(:backend pg
                             :host "127.0.0.1"
                             :port 5432
                             :user "alice"
                             :database "appdb"
                             :pass-entry "alpha"
                             :tramp-default-directory
                             "/docker:vscode@f500f94f96e3:/workspace/")))
            (should (equal clutch--connection-params built))
            (should (eq (current-buffer) (get-buffer buffer-name)))))
      (when-let* ((buf (get-buffer buffer-name)))
        (kill-buffer buf)))))

(ert-deftest clutch-test-connect-in-ad-hoc-sqlite-console-reuses-file-params ()
  "Ad hoc SQLite consoles should reconnect using their file params."
  (with-temp-buffer
    (let ((params '(:backend sqlite :database "/tmp/ad-hoc.db"))
          built
          read-called)
      (clutch-mode)
      (setq-local clutch--console-name "SQLite: /tmp/ad-hoc.db"
                  clutch--console-ad-hoc-params params)
      (cl-letf (((symbol-function 'clutch--connection-alive-p)
                 (lambda (_conn) nil))
                ((symbol-function 'clutch--read-connection-params)
                 (lambda ()
                   (setq read-called t)
                   '(:backend mysql :database "should-not-be-used")))
                ((symbol-function 'clutch--effective-sql-product)
                 (lambda (_params) 'sqlite))
                ((symbol-function 'clutch--build-conn)
                 (lambda (conn-params)
                   (setq built conn-params)
                   'new-conn))
                ((symbol-function 'clutch--connection-key)
                 (lambda (_conn) "sqlite-file"))
                ((symbol-function 'clutch--activate-current-buffer-connection)
                 (lambda (_conn _params _product) nil))
                ((symbol-function 'message) #'ignore))
        (clutch-connect)
        (should-not read-called)
        (should (equal built params))))))

(ert-deftest clutch-test-query-console-switches-to-existing-connected-buffer ()
  "Query console should reuse an existing connected console buffer."
  (let* ((name "alpha")
         (existing (get-buffer-create " *clutch-query-console-existing*"))
         (clutch-connection-alist '(("alpha" . (:backend mysql :database "app_a"))))
         built)
    (unwind-protect
        (with-current-buffer existing
          (clutch-mode)
          (setq-local clutch--console-name name)
          (setq-local clutch-connection 'live-conn)
          (cl-letf (((symbol-function 'clutch--find-console-buffer)
                     (lambda (&rest _args) existing))
                    ((symbol-function 'clutch--connection-alive-p)
                     (lambda (conn) (eq conn 'live-conn)))
                    ((symbol-function 'clutch--update-console-buffer-name)
                     (lambda () nil))
                    ((symbol-function 'clutch--build-conn)
                     (lambda (_params)
                       (setq built t)
                       'unexpected-conn)))
            (clutch-query-console name)
            (should-not built)
            (should (eq (current-buffer) existing))))
      (when (buffer-live-p existing)
        (kill-buffer existing)))))

(ert-deftest clutch-test-query-console-rename-reuses-open-buffer-by-storage-identity ()
  "Renaming a saved connection should keep using the open console buffer."
  (let* ((old-name "alpha")
         (new-name "beta")
         (params '(:backend mysql
                   :host "db.internal"
                   :port 3306
                   :user "app"
                   :database "prod"))
         (storage-name (clutch--console-persistence-name old-name params))
         (existing (get-buffer-create " *clutch-query-console-renamed*"))
         (clutch-connection-alist `((,new-name . ,params)))
         built
         renamed-to)
    (unwind-protect
        (with-current-buffer existing
          (clutch-mode)
          (setq-local clutch--console-name old-name)
          (setq-local clutch--console-storage-name storage-name)
          (setq-local clutch-connection 'live-conn)
          (cl-letf (((symbol-function 'clutch--connection-alive-p)
                     (lambda (conn) (eq conn 'live-conn)))
                    ((symbol-function 'clutch--build-conn)
                     (lambda (_params)
                       (setq built t)
                       'unexpected-conn))
                    ((symbol-function 'clutch--update-console-buffer-name)
                     (lambda ()
                       (setq renamed-to clutch--console-name))))
            (clutch-query-console new-name)
            (should-not built)
            (should (eq (current-buffer) existing))
            (should (equal clutch--console-name new-name))
            (should (equal clutch--console-storage-name storage-name))
            (should (equal renamed-to new-name))))
      (when (buffer-live-p existing)
        (kill-buffer existing)))))

(ert-deftest clutch-test-find-console-buffer-prefers-storage-identity-over-alias ()
  "Console lookup should prefer stable identity over a stale alias match."
  (let* ((name "beta")
         (storage-name "console-stable")
         (wrong (get-buffer-create " *clutch-query-console-wrong-alias*"))
         (right (get-buffer-create " *clutch-query-console-right-storage*")))
    (unwind-protect
        (progn
          (with-current-buffer wrong
            (clutch-mode)
            (setq-local clutch--console-name name)
            (setq-local clutch--console-storage-name "console-other"))
          (with-current-buffer right
            (clutch-mode)
            (setq-local clutch--console-name "alpha")
            (setq-local clutch--console-storage-name storage-name))
          (should (eq (clutch--find-console-buffer name storage-name) right)))
      (when (buffer-live-p wrong)
        (kill-buffer wrong))
      (when (buffer-live-p right)
        (kill-buffer right)))))

(ert-deftest clutch-test-find-console-buffer-ignores-alias-with-different-storage-identity ()
  "Alias fallback should not reuse a console for a different connection identity."
  (let ((buf (get-buffer-create " *clutch-query-console-different-storage*")))
    (unwind-protect
        (progn
          (with-current-buffer buf
            (clutch-mode)
            (setq-local clutch--console-name "alpha")
            (setq-local clutch--console-storage-name "console-old"))
          (should-not (clutch--find-console-buffer "alpha" "console-new")))
      (when (buffer-live-p buf)
        (kill-buffer buf)))))

(ert-deftest clutch-test-find-console-buffer-matches-legacy-buffer-by-params ()
  "Open legacy console buffers without storage state should still match by params."
  (let* ((params '(:backend mysql
                   :host "db.internal"
                   :port 3306
                   :user "app"
                   :database "prod"))
         (storage-name (clutch--console-persistence-name "beta" params))
         (buf (get-buffer-create " *clutch-query-console-legacy-buffer*")))
    (unwind-protect
        (progn
          (with-current-buffer buf
            (clutch-mode)
            (setq-local clutch--console-name "alpha")
            (setq-local clutch--connection-params params))
          (should (eq (clutch--find-console-buffer "beta" storage-name) buf)))
      (when (buffer-live-p buf)
        (kill-buffer buf)))))

(ert-deftest clutch-test-find-console-buffer-ignores-legacy-alias-with-different-params ()
  "Legacy alias fallback should not override a known connection identity."
  (let* ((old-params '(:backend mysql
                       :host "db-a.internal"
                       :port 3306
                       :user "app"
                       :database "prod"))
         (new-params '(:backend mysql
                       :host "db-b.internal"
                       :port 3306
                       :user "app"
                       :database "prod"))
         (storage-name (clutch--console-persistence-name "alpha" new-params))
         (buf (get-buffer-create " *clutch-query-console-legacy-mismatch*")))
    (unwind-protect
        (progn
          (with-current-buffer buf
            (clutch-mode)
            (setq-local clutch--console-name "alpha")
            (setq-local clutch--connection-params old-params))
          (should-not (clutch--find-console-buffer "alpha" storage-name)))
      (when (buffer-live-p buf)
        (kill-buffer buf)))))

(ert-deftest clutch-test-query-console-reconnects-dead-existing-buffer ()
  "Query console should reconnect an existing dead console before switching."
  (let* ((name "alpha")
         (existing (get-buffer-create " *clutch-query-console-dead*"))
         (clutch-connection-alist '(("alpha" . (:backend mysql :database "app_a"))))
         built
         activated)
    (unwind-protect
        (with-current-buffer existing
          (clutch-mode)
          (setq-local clutch--console-name name)
          (setq-local clutch-connection 'dead-conn)
          (cl-letf (((symbol-function 'clutch--find-console-buffer)
                     (lambda (&rest _args) existing))
                    ((symbol-function 'clutch--connection-alive-p)
                     (lambda (_conn) nil))
                    ((symbol-function 'clutch--effective-sql-product)
                     (lambda (_params) 'mysql))
                    ((symbol-function 'clutch--build-conn)
                     (lambda (params)
                       (setq built params)
                       'new-conn))
                    ((symbol-function 'clutch--update-console-buffer-name)
                     (lambda () nil))
                    ((symbol-function 'clutch--activate-current-buffer-connection)
                     (lambda (conn params product)
                       (setq-local clutch-connection conn)
                       (setq activated (list conn params product)))))
            (clutch-query-console name)
            (should (equal built '(:backend mysql :database "app_a" :pass-entry "alpha")))
            (should (equal activated
                           '(new-conn (:backend mysql :database "app_a" :pass-entry "alpha") mysql)))
            (should (eq (current-buffer) existing))))
      (when (buffer-live-p existing)
        (kill-buffer existing)))))

(ert-deftest clutch-test-query-console-loads-legacy-alias-file-when-identity-file-missing ()
  "Query console should migrate smoothly from alias-based persistence."
  (let* ((name "alpha")
         (dir (make-temp-file "clutch-console-" t))
         (buffer-name (clutch--console-buffer-base-name name))
         (params '(:backend mysql
                   :host "db.internal"
                   :port 3306
                   :user "app"
                   :database "prod"))
         (clutch-console-directory dir)
         (clutch-connection-alist `((,name . ,params))))
    (unwind-protect
        (progn
          (with-temp-file (expand-file-name "alpha.sql" dir)
            (insert "SELECT legacy;"))
          (cl-letf (((symbol-function 'clutch--effective-sql-product)
                     (lambda (_params) 'mysql))
                    ((symbol-function 'clutch--build-conn)
                     (lambda (_params) 'conn))
                    ((symbol-function 'clutch--activate-current-buffer-connection)
                     (lambda (_conn _params _product) nil))
                    ((symbol-function 'clutch--update-console-buffer-name)
                     (lambda () nil)))
            (clutch-query-console name)
            (should (equal (buffer-string) "SELECT legacy;"))
            (should-not (equal clutch--console-storage-name name))))
      (when-let* ((buf (get-buffer buffer-name)))
        (kill-buffer buf))
      (delete-directory dir t))))

(ert-deftest clutch-test-query-console-prefers-identity-file-over-legacy-alias-file ()
  "Existing identity-keyed console files should win over legacy alias files."
  (let* ((name "alpha")
         (dir (make-temp-file "clutch-console-" t))
         (buffer-name (clutch--console-buffer-base-name name))
         (params '(:backend mysql
                   :host "db.internal"
                   :port 3306
                   :user "app"
                   :database "prod"))
         (storage-name (clutch--console-persistence-name name params))
         (clutch-console-directory dir)
         (clutch-connection-alist `((,name . ,params))))
    (unwind-protect
        (progn
          (with-temp-file (expand-file-name "alpha.sql" dir)
            (insert "SELECT legacy;"))
          (with-temp-file (clutch--console-file storage-name)
            (insert "SELECT stable;"))
          (cl-letf (((symbol-function 'clutch--effective-sql-product)
                     (lambda (_params) 'mysql))
                    ((symbol-function 'clutch--build-conn)
                     (lambda (_params) 'conn))
                    ((symbol-function 'clutch--activate-current-buffer-connection)
                     (lambda (_conn _params _product) nil))
                    ((symbol-function 'clutch--update-console-buffer-name)
                     (lambda () nil)))
            (clutch-query-console name)
            (should (equal (buffer-string) "SELECT stable;"))))
      (when-let* ((buf (get-buffer buffer-name)))
        (kill-buffer buf))
      (delete-directory dir t))))

(ert-deftest clutch-test-connect-outside-console-still-uses-generic-read-flow ()
  "Non-console buffers should keep the generic interactive connect flow."
  (with-temp-buffer
    (let ((clutch-connection-alist '(("alpha" . (:backend mysql :database "app_a"))))
          built)
      (cl-letf (((symbol-function 'clutch--connection-alive-p)
                 (lambda (_conn) nil))
                ((symbol-function 'clutch--read-connection-params)
                 (lambda () '(:backend mysql :database "manual_db")))
                ((symbol-function 'clutch--effective-sql-product)
                 (lambda (_params) 'mysql))
                ((symbol-function 'clutch--build-conn)
                 (lambda (params)
                   (setq built params)
                   'new-conn))
                ((symbol-function 'clutch--connection-key)
                 (lambda (_conn) "test-conn"))
                ((symbol-function 'clutch--activate-current-buffer-connection)
                 (lambda (_conn _params _product) nil))
                ((symbol-function 'message) #'ignore))
        (clutch-connect)
        (should (equal built '(:backend mysql :database "manual_db")))))))

;;;; Debug — error humanization

(ert-deftest clutch-test-humanize-db-error-known-patterns ()
  "Known backend errors should be cleaned and hinted."
  (dolist (case `((clickhouse-update
                   ,(concat "Code: 48. DB::Exception: Lightweight updates are not supported. "
                            "Lightweight updates are supported only for tables with materialized "
                            "_block_number column. (NOT_IMPLEMENTED) "
                            "(version 26.2.5.45 (official build))  "
                            "(queryId= 92961bbf-e430-4c89-95c3-0deb471daec6)")
                   ("enable lightweight update")
                   ("queryId" "version 26"))
                  (clickhouse-delete-singular
                   "Lightweight delete is not supported"
                   ("enable lightweight delete")
                   nil)
                  (clickhouse-delete-plural
                   "Lightweight deletes are not supported"
                   ("enable lightweight delete")
                   nil)
                  (oracle-ora00942
                   "ORA-00942: table or view does not exist"
                   ("ORA-00942" "table or view does not exist")
                   nil)
                  (connection-refused
                   "Connection refused (host=db.example.com, port=3306)"
                   ("check host and port")
                   nil)
                  (jdbc-driver-missing
                   "SQLException [SQLState=08001]: No suitable driver found for jdbc:oracle:thin:@//db:1521/ORCL"
                   ("No suitable driver found"
                    "clutch-jdbc-install-driver RET oracle")
                   nil)))
    (pcase-let ((`(,label ,msg ,present-patterns ,absent-patterns) case))
      (ert-info ((format "case: %s" label))
        (let ((result (clutch--humanize-db-error msg)))
          (dolist (pattern present-patterns)
            (should (string-match-p pattern result)))
          (dolist (pattern absent-patterns)
            (should-not (string-match-p pattern result))))))))

(ert-deftest clutch-test-humanize-db-error-parts-keep-jdbc-state-in-summary ()
  "Structured error rendering should not treat JDBC SQLState as a display hint."
  (let ((parts (clutch--humanize-db-error-parts
                "SQLException [SQLState=99999]: unexpected driver error")))
    (should (string-match-p "\\[SQLState=99999\\]"
                            (plist-get parts :summary)))
    (should-not (plist-get parts :hint))))

(ert-deftest clutch-test-humanize-db-error-strips-noise ()
  "Unknown and noisy errors should keep the useful summary only."
  (dolist (case `((unknown
                   "Something totally unexpected happened"
                   "Something totally unexpected happened"
                   nil
                   ("\\["))
                  (clickhouse-suffix
                   "Some error (version 24.1.1.1 (official build)) (queryId= abc-123)"
                   nil
                   ("Some error")
                   ("queryId" "version 24"))
                  (java-stack
                   ,(concat "Connection failed: timeout\n"
                            "\tat java.base/java.net.Socket.connect(Socket.java:633)\n"
                            "\tat com.clickhouse.client.Http.open(Http.java:42)")
                   nil
                   ("Connection failed")
                   ("java\\.base" "Socket"))
                  (database-prefix
                   "Database error: ORA-00942: table does not exist"
                   nil
                   ("ORA-00942")
                   ("^Database error:"))))
    (pcase-let ((`(,label ,msg ,expected ,present-patterns ,absent-patterns)
                 case))
      (ert-info ((format "case: %s" label))
        (let ((result (clutch--humanize-db-error msg)))
          (when expected
            (should (equal result expected)))
          (dolist (pattern present-patterns)
            (should (string-match-p pattern result)))
          (dolist (pattern absent-patterns)
            (should-not (string-match-p pattern result))))))))

;;;; Debug — problem records and buffer

(ert-deftest clutch-test-debug-buffer-removes-old-details-commands ()
  "The old error-details workflow should be deleted outright."
  (should-not (fboundp 'clutch-show-last-error-details))
  (should-not (fboundp 'clutch-show-error-details))
  (should-not (fboundp 'clutch-debug-buffer))
  (should-not (fboundp 'clutch-error-details-refresh))
  (should-not (fboundp 'clutch-error-details-copy-message))
  (should-not (fboundp 'clutch-error-details-copy-all))
  (should-not (boundp 'clutch-error-details-mode-map)))

(ert-deftest clutch-test-debug-mode-creates-dedicated-buffer ()
  "Enabling debug mode should create the dedicated debug buffer immediately."
  (when-let* ((buf (get-buffer clutch-debug-buffer-name)))
    (kill-buffer buf))
  (let ((clutch-debug-mode nil))
    (unwind-protect
        (progn
          (clutch-debug-mode 1)
          (let ((buf (get-buffer clutch-debug-buffer-name)))
            (should (buffer-live-p buf))
            (with-current-buffer buf
              (should (derived-mode-p 'clutch--debug-buffer-mode))
              (should (string-match-p "Clutch Debug"
                                      (buffer-string))))))
      (clutch-debug-mode -1)
      (when-let* ((buf (get-buffer clutch-debug-buffer-name)))
        (kill-buffer buf)))))

(ert-deftest clutch-test-debug-buffer-appends-current-buffer-failure ()
  "Problem records should be appended to the dedicated debug buffer."
  (let ((details '(:backend jdbc
                   :summary "Connection failed [check host and port]"
                   :diag (:category "connect"
                          :op "connect"
                          :request-id 71
                          :exception-class "java.sql.SQLNonTransientConnectionException"
                          :sql-state "08071"
                          :context (:redacted-url "jdbc:clickhouse://127.0.0.1:8123/testdb?password=<redacted>"
                                    :generated-sql "ALTER SESSION SET CURRENT_SCHEMA = \"REPORTING\""
                                    :property-keys ("http_header_COOKIE" "socket_timeout"))
                          :cause-chain ((:exception-class "java.sql.SQLNonTransientConnectionException"
                                         :message "reason-71")
                                        (:exception-class "java.net.ConnectException"
                                         :message "root-71")))
                   :stderr-tail "Connect request 71 failed")))
    (with-temp-buffer
      (let ((clutch-debug-mode t))
        (clutch--clear-debug-capture)
        (clutch--remember-problem-record
         :buffer (current-buffer)
         :problem details)
        (let ((text (clutch-test--debug-buffer-string)))
          (should (string-match-p "Connection failed" text))
          (should (string-match-p "08071" text))
          (should (string-match-p "ALTER SESSION SET CURRENT_SCHEMA" text))
          (should (string-match-p "Connect request 71 failed" text)))))))

(ert-deftest clutch-test-debug-buffer-appends-connection-problem-record ()
  "Connection-scoped problem records should also be appended to the debug buffer."
  (let ((problem '(:backend oracle
                   :summary "Metadata failed"
                   :diag (:category "metadata"
                          :op "get-columns"
                          :conn-id 88
                          :raw-message "ORA-12592: TNS:bad packet"))))
    (with-temp-buffer
      (let ((clutch-debug-mode t))
        (clutch--clear-debug-capture)
        (setq-local clutch-connection 'fake-conn)
        (clutch--remember-problem-record
         :connection 'fake-conn
         :problem problem)
        (let ((text (clutch-test--debug-buffer-string)))
          (should (string-match-p "Metadata failed" text))
          (should (string-match-p "get-columns" text)))))))

(ert-deftest clutch-test-debug-buffer-appends-debug-trace-when-enabled ()
  "Debug mode should append recent captured events to the debug buffer."
  (let ((details '(:backend jdbc
                   :summary "Query failed"
                   :diag (:category "query"
                          :op "execute"
                          :raw-message "ORA-00942"))))
    (with-temp-buffer
      (let ((clutch-debug-mode t))
        (clutch--clear-debug-capture)
        (setq-local clutch--buffer-error-details details)
        (clutch--remember-debug-event
         :op "execute"
         :phase "error"
         :backend 'oracle
         :summary "Query failed"
         :sql "SELECT * FROM missing_table")
        (let ((text (clutch-test--debug-buffer-string)))
          (should (string-match-p "Trace Event" text))
          (should (string-match-p "Operation: execute" text))
          (should (string-match-p "Phase: error" text))
          (should (string-match-p "Query failed" text))
          (should (string-match-p "SELECT \\* FROM missing_table" text)))))))

(ert-deftest clutch-test-run-db-query-success-clears-problems-across-connection-buffers ()
  "Successful queries should clear stale failure state for the whole connection."
  (let ((conn 'fake-conn)
        (source (generate-new-buffer " *clutch-debug-source*"))
        (peer (generate-new-buffer " *clutch-debug-peer*"))
        cleared)
    (unwind-protect
        (progn
          (with-current-buffer source
            (setq-local clutch-connection conn)
            (clutch--remember-problem-record
             :buffer source
             :connection conn
             :problem '(:summary "old")))
          (with-current-buffer peer
            (setq-local clutch-connection conn)
            (cl-letf (((symbol-function 'clutch-db-query)
                       (lambda (_conn _sql) 'ok))
                      ((symbol-function 'clutch-db-manual-commit-p)
                       (lambda (_conn) nil))
                      ((symbol-function 'clutch-db-clear-error-details)
                       (lambda (clear-conn)
                         (setq cleared clear-conn))))
              (should (eq (clutch--run-db-query conn "SELECT 1") 'ok))))
          (with-current-buffer source
            (should-not clutch--buffer-error-details)
            (should-not (gethash conn clutch--problem-records-by-conn)))
          (should-not (gethash conn clutch--problem-records-by-conn))
          (should (eq cleared conn)))
      (kill-buffer source)
      (kill-buffer peer))))

(ert-deftest clutch-test-debug-mode-enable-replays-stored-problem-records ()
  "Enabling debug mode should replay already-stored problems as history."
  (let ((source (generate-new-buffer " *clutch-debug-replay*"))
        (summary "pre-debug failure"))
    (unwind-protect
        (progn
          (when clutch-debug-mode
            (clutch-debug-mode -1))
          (clutch-test--clear-problem-capture)
          (with-current-buffer source
            (clutch--remember-problem-record
             :buffer source
             :problem `(:backend oracle :summary ,summary)))
          (clutch-debug-mode 1)
          (let ((text (clutch-test--debug-buffer-string)))
            (should (string-match-p "Historical Problems" text))
            (should (string-match-p summary text))))
      (when clutch-debug-mode
        (clutch-debug-mode -1))
      (when (buffer-live-p source)
        (kill-buffer source))
      (when-let* ((buf (get-buffer clutch-debug-buffer-name)))
        (kill-buffer buf)))))

(ert-deftest clutch-test-debug-mode-enable-does-not-clear-problem-records ()
  "Enabling debug mode should keep already-stored problem records."
  (let ((source (generate-new-buffer " *clutch-debug-problem*")))
    (unwind-protect
        (progn
          (when clutch-debug-mode
            (clutch-debug-mode -1))
          (clutch-test--clear-problem-capture)
          (with-current-buffer source
            (clutch--remember-problem-record
             :buffer source
             :problem '(:summary "pre-debug")))
          (clutch-debug-mode 1)
          (with-current-buffer source
            (should (equal (plist-get clutch--buffer-error-details :summary)
                           "pre-debug"))))
      (when clutch-debug-mode
        (clutch-debug-mode -1))
      (when (buffer-live-p source)
        (kill-buffer source))
      (when-let* ((buf (get-buffer clutch-debug-buffer-name)))
        (kill-buffer buf)))))

(ert-deftest clutch-test-debug-mode-enable-clears-debug-events-but-keeps-problems ()
  "Starting a new debug capture should clear events but keep problems."
  (let ((conn 'fake-conn)
        (source (generate-new-buffer " *clutch-debug-capture*")))
    (unwind-protect
        (progn
          (when clutch-debug-mode
            (clutch-debug-mode -1))
          (clutch-test--clear-problem-capture)
          (with-current-buffer source
            (setq-local clutch-connection conn)
            (clutch-debug-mode 1)
            (clutch--remember-problem-record
             :buffer source
             :connection conn
             :problem '(:summary "pre-debug"))
            (clutch--remember-debug-event
             :buffer source
             :connection conn
             :op "execute"
             :phase "error"
             :summary "boom")
            (should clutch--debug-events)
            (should clutch--buffer-error-details)
            (clutch-debug-mode -1)
            (clutch-debug-mode 1)
            (should-not clutch--debug-events)
            (should clutch--buffer-error-details)))
      (when clutch-debug-mode
        (clutch-debug-mode -1))
      (when (buffer-live-p source)
        (kill-buffer source))
      (when-let* ((buf (get-buffer clutch-debug-buffer-name)))
        (kill-buffer buf)))))

(ert-deftest clutch-test-execute-error-populates-problem-record ()
  "SQL execution failures should populate the current buffer problem record."
  (let ((source (generate-new-buffer " *clutch-error-source*")))
    (unwind-protect
        (with-current-buffer source
          (set-window-buffer (selected-window) source)
          (setq-local clutch-connection 'fake-conn
                      clutch--source-window (selected-window))
          (cl-letf (((symbol-function 'clutch-db-build-paged-sql)
                     (lambda (_conn sql _page-num _page-size
                                    &optional _order-by _page-offset)
                       sql))
                    ((symbol-function 'clutch--run-db-query)
                     (lambda (_conn _sql)
                       (signal 'clutch-db-error
                               (list "ORA-00942: table or view does not exist"))))
                    ((symbol-function 'clutch-db-error-details)
                     (lambda (_conn) nil))
                    ((symbol-function 'pop-to-buffer)
                     (lambda (buf &rest _args) buf)))
            (catch 'clutch--execution-aborted
              (clutch--execute-select "SELECT * FROM missing_table" 'fake-conn))
            (let* ((details clutch--buffer-error-details)
                   (diag (plist-get details :diag)))
              (should details)
              (should (equal (plist-get details :summary)
                             (clutch--humanize-db-error
                              "ORA-00942: table or view does not exist")))
              (should (equal (plist-get diag :raw-message)
                             "ORA-00942: table or view does not exist"))
              (should (equal (plist-get (plist-get diag :context) :sql)
                             "SELECT * FROM missing_table")))))
      (kill-buffer source))))

;;;; Execute — query execution and error handling

(ert-deftest clutch-test-collect-all-export-rows-paged ()
  "Collect filtered export rows by paging without losing row identity."
  (with-temp-buffer
    (let (captured-base)
      (setq-local clutch-connection 'fake-conn
                  clutch--base-query "SELECT name FROM t"
                  clutch--last-query "SELECT name FROM t"
                  clutch--where-filter "name LIKE 'ann%'"
                  clutch--result-source-table "t"
                  clutch--result-server-pageable t
                  clutch--order-by nil
                  clutch--row-identity
                  (clutch-test--primary-row-identity "t" '("id") '(1)))
      (let ((clutch-result-max-rows 2))
        (cl-letf (((symbol-function 'clutch--connection-alive-p)
                   (lambda (_conn) t))
                  ((symbol-function 'clutch-db-apply-where)
                   (lambda (_conn sql filter)
                     (format "FILTER[%s]{%s}" filter sql)))
                  ((symbol-function 'clutch-db-escape-identifier)
                   (lambda (_conn id) (format "`%s`" id)))
                  ((symbol-function 'clutch-db-build-paged-sql)
                   (lambda (_conn sql page-num _page-size _order-by
                           &optional _page-offset)
                     (unless captured-base
                       (setq captured-base sql))
                     (format "SELECT name FROM t -- page:%d" page-num)))
                  ((symbol-function 'clutch-db-query)
                   (lambda (_conn sql)
                     (let ((rows (cond ((string-match-p "page:0\\'" sql)
                                        '(("ann" 1) ("anna" 2)))
                                       ((string-match-p "page:1\\'" sql)
                                        '(("annie" 3)))
                                       (t nil))))
                       (make-clutch-db-result :rows rows)))))
          (should (equal (clutch-result--collect-all-export-rows)
                         '(("ann" 1) ("anna" 2) ("annie" 3))))
          (should (equal captured-base
                         "FILTER[name LIKE 'ann%']{SELECT name, `id` AS `clutch__rid_0` FROM t}")))))))

(ert-deftest clutch-test-collect-all-export-rows-with-top-level-limit ()
  "Export should reuse already fetched rows for nonpageable limited results."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn)
    (setq-local clutch--base-query "SELECT id FROM t LIMIT 2")
    (setq-local clutch--last-query "SELECT id FROM t LIMIT 2")
    (setq-local clutch--where-filter nil)
    (setq-local clutch--result-server-pageable nil)
    (setq-local clutch--result-rows '((1) (2)))
    (let ((calls 0))
      (cl-letf (((symbol-function 'clutch--connection-alive-p)
                 (lambda (_conn) t))
                ((symbol-function 'clutch-db-build-paged-sql)
                 (lambda (&rest _args)
                   (error "Should not paginate")))
                ((symbol-function 'clutch-db-query)
                 (lambda (_conn _sql)
                   (cl-incf calls)
                   (make-clutch-db-result :rows '((1) (2))))))
        (should (equal (clutch-result--collect-all-export-rows) '((1) (2))))
        (should (= calls 0))))))

(ert-deftest clutch-test-collect-all-export-rows-with-top-level-limit-and-sort ()
  "Export should reuse current rows when limited results are not pageable."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn)
    (setq-local clutch--base-query "SELECT id FROM t LIMIT 3")
    (setq-local clutch--last-query "SELECT id FROM t LIMIT 3")
    (setq-local clutch--where-filter nil)
    (setq-local clutch--order-by '("id" . "DESC"))
    (setq-local clutch--result-server-pageable nil)
    (setq-local clutch--result-rows '((3) (2) (1)))
    (let ((clutch-result-max-rows 2)
          seen-order-by)
      (cl-letf (((symbol-function 'clutch--connection-alive-p)
                (lambda (_conn) t))
                ((symbol-function 'clutch-db-build-paged-sql)
                 (lambda (_conn _sql page-num _page-size order-by
                         &optional _page-offset)
                   (setq seen-order-by order-by)
                   (format "SELECT id FROM t LIMIT 3 -- page:%d" page-num)))
                ((symbol-function 'clutch-db-query)
                 (lambda (_conn sql)
                   (make-clutch-db-result
                    :rows (cond
                           ((string-match-p "page:0\\'" sql) '((3) (2)))
                           ((string-match-p "page:1\\'" sql) '((1)))
                           (t (error "unsorted direct query: %s" sql)))))))
        (should (equal (clutch-result--collect-all-export-rows)
                       '((3) (2) (1))))
        (should-not seen-order-by)))))

(ert-deftest clutch-test-collect-all-export-rows-ensures-connection ()
  "Export-all should use the shared reconnect path before querying."
  (with-temp-buffer
    (let (ensured captured-conn)
      (setq-local clutch-connection 'stale-conn
                  clutch--base-query "SELECT id FROM t LIMIT 1"
                  clutch--last-query "SELECT id FROM t LIMIT 1"
                  clutch--result-server-pageable t)
      (cl-letf (((symbol-function 'clutch--ensure-connection)
                 (lambda ()
                   (setq ensured t)
                   (setq-local clutch-connection 'new-conn)))
                ((symbol-function 'clutch-db-query)
                 (lambda (conn _sql)
                   (setq captured-conn conn)
                   (make-clutch-db-result :rows '((1))))))
        (should (equal (clutch-result--collect-all-export-rows) '((1))))
        (should ensured)
        (should (eq captured-conn 'new-conn))))))

(ert-deftest clutch-test-execute-select-detects-primary-key-before-first-render ()
  "Primary key cache should be ready before the first result render."
  (let ((clutch--source-window (selected-window))
        (result-name "*clutch-test-result*")
        (captured-pk :unset))
    (cl-letf (((symbol-function 'clutch-db-build-paged-sql)
               (lambda (_conn sql _page-num _page-size
                              &optional _order-by _page-offset)
                 sql))
              ((symbol-function 'clutch-db-row-identity-candidates)
               (lambda (_conn _table)
                 (list (list :kind 'primary-key
                             :name "PRIMARY"
                             :columns '("id")))))
              ((symbol-function 'clutch-db-escape-identifier)
               (lambda (_conn id) (format "\"%s\"" id)))
              ((symbol-function 'clutch-db-query)
               (lambda (_conn _sql)
                 (make-clutch-db-result
                  :columns '((:name "id") (:name "name") (:name "clutch__rid_0"))
                  :rows '((1 "a" 1))))))
      (clutch-test--with-result-buffer
          (result-name (lambda (&rest _args)
                         (setq captured-pk clutch--cached-pk-indices)))
        (clutch--execute-select "SELECT * FROM users" 'fake-conn)
        (should (equal captured-pk '(0)))
        (with-current-buffer result-name
          (should clutch--result-server-pageable)
          (should clutch--result-server-rewritable))))))

(ert-deftest clutch-test-execute-select-fetches-one-row-lookahead ()
  "Initial SELECT execution should trim lookahead rows before rendering."
  (let ((clutch--source-window (selected-window))
        (result-name "*clutch-test-result*")
        (clutch-result-max-rows 2)
        captured-page-size
        captured-offset)
    (cl-letf (((symbol-function 'clutch-db-build-paged-sql)
               (lambda (_conn sql _page-num page-size &optional _order-by page-offset)
                 (setq captured-page-size page-size
                       captured-offset page-offset)
                 sql))
              ((symbol-function 'clutch-db-row-identity-candidates)
               (lambda (&rest _args) nil))
              ((symbol-function 'clutch-db-query)
               (lambda (_conn _sql)
                 (make-clutch-db-result
                  :columns '((:name "id"))
                  :rows '((1) (2) (3))))))
      (clutch-test--with-result-buffer (result-name)
        (clutch--execute-select "SELECT id FROM users" 'fake-conn)
        (should (= captured-page-size 3))
        (should (= captured-offset 0))
        (with-current-buffer result-name
          (should (equal clutch--result-rows '((1) (2))))
          (should clutch--page-has-more)
          (should (= clutch--page-offset 0)))))))

(ert-deftest clutch-test-execute-select-page-tailed-queries-stay-flat ()
  "Page-tailed SELECT shapes should execute directly without wrapper paging."
  (dolist (case `((offset
                   "SELECT id FROM users OFFSET 20"
                   ((:name "id"))
                   ((1))
                   nil)
                  (complex-limit
                   "SELECT c.*, cc.* FROM table_a AS c JOIN table_b AS cc ON c.id = cc.id LIMIT 10"
                   ((:name "id") (:name "name") (:name "id"))
                   ((1 "a" 1) (2 "b" 2))
                   ((1 "a" 1) (2 "b" 2)))
                  (duplicate-label-limit
                   "SELECT id AS dup, name AS dup FROM users LIMIT 10"
                   ((:name "dup") (:name "dup"))
                   ((1 "a"))
                   nil)))
    (pcase-let ((`(,label ,sql ,columns ,rows ,expected-rows) case))
      (ert-info ((format "case: %s" label))
        (let ((clutch--source-window (selected-window))
              (result-name "*clutch-test-result*")
              captured-sql)
          (cl-letf (((symbol-function 'clutch-db-build-paged-sql)
                     (lambda (&rest _args)
                       (error "Should not wrap page-tailed query results")))
                    ((symbol-function 'clutch-db-row-identity-candidates)
                     (lambda (&rest _args) nil))
                    ((symbol-function 'clutch--run-db-query)
                     (lambda (_conn query)
                       (setq captured-sql query)
                       (make-clutch-db-result
                        :columns columns
                        :rows rows))))
            (clutch-test--with-result-buffer (result-name)
              (clutch--execute-select sql 'fake-conn)
              (should (equal captured-sql sql))
              (with-current-buffer result-name
                (should-not clutch--result-server-pageable)
                (should-not clutch--result-server-rewritable)
                (should-not clutch--page-has-more)
                (when expected-rows
                  (should (equal clutch--result-rows expected-rows)))))))))))

(ert-deftest clutch-test-execute-select-duplicate-labels-are-not-rewritable ()
  "Duplicate result labels should remain pageable but not relation-rewritable."
  (let ((clutch--source-window (selected-window))
        (result-name "*clutch-test-result*")
        (sql "SELECT id AS dup, name AS dup FROM users")
        captured-build-sql)
    (cl-letf (((symbol-function 'clutch-db-build-paged-sql)
               (lambda (_conn base-sql _page-num _page-size
                              &optional _order-by _page-offset)
                 (setq captured-build-sql base-sql)
                 base-sql))
              ((symbol-function 'clutch-db-row-identity-candidates)
               (lambda (&rest _args) nil))
              ((symbol-function 'clutch--run-db-query)
               (lambda (_conn _query)
                 (make-clutch-db-result
                  :columns '((:name "dup") (:name "dup"))
                  :rows '((1 "a"))))))
      (clutch-test--with-result-buffer (result-name)
        (clutch--execute-select sql 'fake-conn)
        (should (equal captured-build-sql sql))
        (with-current-buffer result-name
          (should clutch--result-server-pageable)
          (should-not clutch--result-server-rewritable))))))

(ert-deftest clutch-test-execute-select-honors-result-context-overrides ()
  "Internal filter SQL should keep the verified relation source capabilities."
  (let ((clutch--source-window (selected-window))
        (result-context
         '(:server-pageable t
           :row-identity-prep
           (:sql "SELECT id, name, `id` AS `clutch__rid_0` FROM users")))
        (result-name "*clutch-test-result*")
        (sql "SELECT * FROM (SELECT id, name FROM users) AS _clutch_filter WHERE id > 1")
        captured-base-sql)
    (cl-letf (((symbol-function 'clutch-db-build-paged-sql)
               (lambda (_conn base-sql _page-num _page-size
                              &optional _order-by _page-offset)
                 (setq captured-base-sql base-sql)
                 base-sql))
              ((symbol-function 'clutch-db-row-identity-candidates)
               (lambda (&rest _args)
                 (error "Should use context row identity prep")))
              ((symbol-function 'clutch--run-db-query)
               (lambda (_conn _query)
                 (make-clutch-db-result
                  :columns '((:name "id") (:name "name"))
                  :rows '((2 "bob"))))))
      (clutch-test--with-result-buffer (result-name)
        (clutch--execute-select sql 'fake-conn result-context)
        (should (string-match-p "`id` AS `clutch__rid_0`"
                                captured-base-sql))))))

(ert-deftest clutch-test-execute-select-remembers-result-buffer-on-source-buffer ()
  "SELECT execution should attach the result buffer to the source buffer."
  (let ((source (generate-new-buffer " *clutch-source*"))
        (result-name "*clutch-test-result*"))
    (unwind-protect
        (progn
          (with-current-buffer source
            (set-window-buffer (selected-window) source)
            (let ((clutch--source-window (selected-window)))
              (cl-letf (((symbol-function 'clutch-db-build-paged-sql)
                         (lambda (_conn sql _page-num _page-size
                                        &optional _order-by _page-offset)
                           sql))
                        ((symbol-function 'clutch-db-row-identity-candidates)
                         (lambda (&rest _args) nil))
                        ((symbol-function 'clutch-db-query)
                         (lambda (_conn _sql)
                           (make-clutch-db-result
                            :columns '((:name "id"))
                            :rows '((1))))))
                (clutch-test--with-result-buffer (result-name)
                  (clutch--execute-select "SELECT id FROM users" 'fake-conn)
                  (should (eq clutch--last-result-buffer
                              (get-buffer result-name))))))))
      (when (buffer-live-p source)
        (kill-buffer source)))))

(ert-deftest clutch-test-require-risky-dml-confirmation-cancels ()
  "Risky DML should be cancelled unless user types YES."
  (cl-letf (((symbol-function 'clutch--risky-dml-reason) (lambda (_sql) "no WHERE"))
            ((symbol-function 'read-string) (lambda (&rest _args) "NO")))
    (should-error (clutch--require-risky-dml-confirmation "UPDATE users SET x=1")
                  :type 'user-error)))

(ert-deftest clutch-test-require-risky-dml-confirmation-accepts-yes ()
  "Risky DML should proceed when user types YES."
  (cl-letf (((symbol-function 'clutch--risky-dml-reason) (lambda (_sql) "no WHERE"))
            ((symbol-function 'read-string) (lambda (&rest _args) "YES")))
    (should (null (clutch--require-risky-dml-confirmation "UPDATE users SET x=1")))))

(ert-deftest clutch-test-preview-execution-sql-prefers-pending-edits-in-result-mode ()
  "Preview in result mode should show generated UPDATE SQL when edits exist."
  (with-temp-buffer
    (let (captured)
      (setq-local clutch-connection 'fake-conn
                  clutch--pending-edits '(((0 . 1) . "v")))
      (cl-letf (((symbol-function 'derived-mode-p) (lambda (&rest _modes) t))
                ((symbol-function 'clutch-result--build-update-statements)
                 (lambda () '(("UPDATE t SET name=? WHERE id=1" . ("v")))))
                ((symbol-function 'clutch-db-escape-literal)
                 (lambda (_conn value) (format "'%s'" value)))
                ((symbol-function 'clutch--preview-sql-buffer)
                 (lambda (sql) (setq captured sql))))
        (clutch-preview-execution-sql)
        (should (string-match-p "UPDATE t SET name='v' WHERE id=1;" captured))))))

(ert-deftest clutch-test-preview-execution-sql-uses-pending-batch-in-result-mode ()
  "Preview in result mode should mirror the staged commit batch."
  (with-temp-buffer
    (let (captured)
      (setq-local clutch--pending-inserts '(a)
                  clutch--pending-edits '(b)
                  clutch--pending-deletes '(c))
      (cl-letf (((symbol-function 'derived-mode-p) (lambda (&rest _modes) t))
                ((symbol-function 'clutch-result--build-pending-insert-statements)
                 (lambda () '(("INSERT INTO t VALUES (1)" . nil))))
                ((symbol-function 'clutch-result--build-update-statements)
                 (lambda () '(("UPDATE t SET name='' WHERE id=1" . nil))))
                ((symbol-function 'clutch-result--build-pending-delete-statements)
                 (lambda () '(("DELETE FROM t WHERE id=1" . nil))))
                ((symbol-function 'clutch--preview-sql-buffer)
                 (lambda (sql) (setq captured sql))))
        (clutch-preview-execution-sql)
        (should (equal captured
                       (mapconcat #'identity
                                  '("INSERT INTO t VALUES (1);"
                                    "UPDATE t SET name='' WHERE id=1;"
                                    "DELETE FROM t WHERE id=1;")
                                  "\n")))))))

(ert-deftest clutch-test-result-effective-query-applies-where-filter ()
  "Result workflows should reuse the filtered SQL, not just display the filter."
  (with-temp-buffer
    (setq-local clutch--base-query "SELECT * FROM t"
                clutch--last-query "SELECT * FROM t WHERE id > 0"
                clutch--where-filter "id = 1")
    (cl-letf (((symbol-function 'clutch-db-apply-where)
               (lambda (_conn sql filter)
                 (format "FILTER[%s]{%s}" filter sql))))
      (should (equal (clutch-result--effective-query)
                     "FILTER[id = 1]{SELECT * FROM t}")))))

(ert-deftest clutch-test-execute-page-uses-effective-filtered-query ()
  "Paging should continue using the active WHERE filter."
  (with-temp-buffer
    (let (captured-base)
      (setq-local clutch-connection 'fake-conn
                  clutch--base-query "SELECT name FROM t"
                  clutch--where-filter "id = 1"
                  clutch--result-source-table "t"
                  clutch--result-server-pageable t
                  clutch--row-identity
                  (clutch-test--primary-row-identity "t" '("id") '(1))
                  clutch-result-max-rows 500)
      (cl-letf (((symbol-function 'clutch-db-apply-where)
                 (lambda (_conn sql filter)
                   (format "FILTER[%s]{%s}" filter sql)))
                ((symbol-function 'clutch-db-escape-identifier)
                 (lambda (_conn id) (format "`%s`" id)))
                ((symbol-function 'clutch-db-row-identity-candidates)
                 (lambda (&rest _)
                   (error "Should reuse result row identity")))
                ((symbol-function 'clutch--connection-alive-p) (lambda (_conn) t))
                ((symbol-function 'clutch-db-build-paged-sql)
                 (lambda (_conn sql _page-num _page-size
                              &optional _order-by _page-offset)
                   (setq captured-base sql)
                   "SELECT * FROM paged"))
                ((symbol-function 'clutch-db-query)
                 (lambda (_conn _sql)
                   (make-clutch-db-result :columns nil :rows nil)))
                ((symbol-function 'clutch--refresh-display) #'ignore)
                ((symbol-function 'message) #'ignore))
        (clutch-result--execute-page 0)
        (should (equal captured-base
                       "FILTER[id = 1]{SELECT name, `id` AS `clutch__rid_0` FROM t}"))))))

(ert-deftest clutch-test-execute-page-fetches-lookahead-and-trims-visible-rows ()
  "Paging should fetch one extra row to distinguish exact last pages."
  (with-temp-buffer
    (let (captured-page-size captured-offset)
      (setq-local clutch-connection 'fake-conn
                  clutch--base-query "SELECT id FROM t"
                  clutch--result-server-pageable t
                  clutch-result-max-rows 2)
      (cl-letf (((symbol-function 'clutch--ensure-connection) #'ignore)
                ((symbol-function 'clutch-db-build-paged-sql)
                 (lambda (_conn _sql _page-num page-size _order-by
                         &optional page-offset)
                   (setq captured-page-size page-size
                         captured-offset page-offset)
                   "SELECT id FROM t LIMIT 3 OFFSET 2"))
                ((symbol-function 'clutch--run-db-query)
                 (lambda (_conn _sql)
                   (make-clutch-db-result
                    :columns '((:name "id"))
                    :rows '((3) (4) (5)))))
                ((symbol-function 'clutch--refresh-display) #'ignore)
                ((symbol-function 'message) #'ignore))
        (clutch-result--execute-page 1)
        (should (= captured-page-size 3))
        (should (= captured-offset 2))
        (should (equal clutch--result-rows '((3) (4))))
        (should clutch--page-has-more)
        (should (= clutch--page-offset 2))
        (should (= clutch--page-current 1))))))

(ert-deftest clutch-test-count-total-uses-effective-filtered-query ()
  "COUNT should run against the filtered SQL when a WHERE filter is active."
  (with-temp-buffer
    (let (captured-base)
      (setq-local clutch-connection 'fake-conn
                  clutch--base-query "SELECT * FROM t"
                  clutch--where-filter "id = 1"
                  clutch--result-server-rewritable t)
      (cl-letf (((symbol-function 'clutch-db-apply-where)
                 (lambda (_conn sql filter)
                   (format "FILTER[%s]{%s}" filter sql)))
                ((symbol-function 'clutch--connection-alive-p) (lambda (_conn) t))
                ((symbol-function 'clutch-db-build-count-sql)
                 (lambda (_conn sql)
                   (setq captured-base sql)
                   "SELECT COUNT(*)"))
                ((symbol-function 'clutch-db-query)
                 (lambda (_conn _sql)
                   (make-clutch-db-result :rows '((7)))))
                ((symbol-function 'clutch--refresh-footer-line) #'ignore)
                ((symbol-function 'message) #'ignore))
        (clutch-result-count-total)
        (should (equal captured-base "FILTER[id = 1]{SELECT * FROM t}"))
        (should (= clutch--page-total-rows 7))))))

(ert-deftest clutch-test-count-total-errors-for-nonrewritable-query-result ()
  "COUNT should not wrap arbitrary query results in a derived table."
  (with-temp-buffer
    (setq-local clutch--result-server-rewritable nil
                clutch-connection 'fake-conn
                clutch--base-query "SELECT a.*, b.* FROM a JOIN b ON a.id = b.id LIMIT 10"
                clutch--last-query clutch--base-query)
    (let (queried)
      (cl-letf (((symbol-function 'clutch--ensure-connection) #'ignore)
                ((symbol-function 'clutch-db-build-count-sql)
                 (lambda (&rest _args) (error "Should not build COUNT SQL")))
                ((symbol-function 'clutch--run-db-query)
                 (lambda (&rest _args) (setq queried t))))
        (let ((err (should-error (clutch-result-count-total)
                                 :type 'user-error)))
          (should (string-match-p "Server-side count"
                                  (error-message-string err))))
        (should-not queried)))))

(ert-deftest clutch-test-execute-page-ensures-connection-before-query ()
  "Paging should use the shared reconnect path before querying."
  (with-temp-buffer
    (let (ensured captured-conn)
      (setq-local clutch-connection 'stale-conn
                  clutch--base-query "SELECT * FROM t"
                  clutch--result-server-pageable t
                  clutch-result-max-rows 100)
      (cl-letf (((symbol-function 'clutch--ensure-connection)
                 (lambda ()
                   (setq ensured t)
                   (setq-local clutch-connection 'new-conn)))
                ((symbol-function 'clutch-db-build-paged-sql)
                 (lambda (_conn _sql _page-num _page-size
                              &optional _order-by _page-offset)
                   "SELECT * FROM paged"))
                ((symbol-function 'clutch-db-query)
                 (lambda (conn _sql)
                   (setq captured-conn conn)
                   (make-clutch-db-result :columns nil :rows nil)))
                ((symbol-function 'clutch--refresh-display) #'ignore)
                ((symbol-function 'message) #'ignore))
        (clutch-result--execute-page 0)
        (should ensured)
        (should (eq captured-conn 'new-conn))))))

(ert-deftest clutch-test-execute-page-remembers-error-details-and-debug-event ()
  "Paging failures should populate `current-buffer' error details and trace."
  (with-temp-buffer
    (let ((clutch-debug-mode t)
          (raw-message "Connection refused (host=db.example.com, port=3306)"))
      (setq-local clutch-connection 'fake-conn
                  clutch--base-query "SELECT * FROM t"
                  clutch--result-server-pageable t
                  clutch-result-max-rows 100)
      (cl-letf (((symbol-function 'clutch--ensure-connection) #'ignore)
                ((symbol-function 'clutch-db-build-paged-sql)
                 (lambda (_conn _sql _page-num _page-size
                              &optional _order-by _page-offset)
                   "SELECT * FROM t LIMIT 100 OFFSET 0"))
                ((symbol-function 'clutch--backend-key-from-conn)
                 (lambda (_conn) 'pg))
                ((symbol-function 'clutch--run-db-query)
                 (lambda (_conn _sql)
                   (signal 'clutch-db-error (list raw-message)))))
        (let ((display-summary
               (condition-case err
                   (signal 'clutch-db-error (list raw-message))
                 (clutch-db-error
                  (clutch--humanize-db-error (error-message-string err))))))
          (should-error (clutch-result--execute-page 0) :type 'user-error)
          (let* ((details clutch--buffer-error-details)
                 (diag (plist-get details :diag))
                 (event (car clutch--debug-events)))
            (should details)
            (should (eq (plist-get details :backend) 'pg))
            (should (equal (plist-get details :summary)
                           (clutch--humanize-db-error raw-message)))
            (should (equal (plist-get diag :raw-message) raw-message))
            (should (equal (plist-get (plist-get diag :context) :sql)
                           "SELECT * FROM t"))
            (should event)
            (should (equal (plist-get event :phase) "error"))
            (should (equal (plist-get event :summary)
                           display-summary))))))))

(ert-deftest clutch-test-execute-dml-skips-debug-backend-lookup-when-disabled ()
  "DML execution should not consult debug-only backend state when debug is off."
  (with-temp-buffer
    (let ((clutch-debug-mode nil)
          rendered)
      (cl-letf (((symbol-function 'clutch--backend-key-from-conn)
                 (lambda (_conn)
                   (error "Debug-disabled path should not resolve backend key")))
                ((symbol-function 'clutch--run-db-query)
                 (lambda (_conn _sql)
                   (make-clutch-db-result :affected-rows 1)))
                ((symbol-function 'clutch-db-sql-schema-affecting-p)
                 (lambda (_sql) nil))
                ((symbol-function 'clutch-result--display)
                 (lambda (result sql _elapsed)
                   (setq rendered (list result sql)))))
        (should (clutch--execute-dml "UPDATE demo SET enabled = 1" 'fake-conn))
        (should (equal (cadr rendered) "UPDATE demo SET enabled = 1"))
        (should (= (clutch-db-result-affected-rows (car rendered)) 1))))))

(ert-deftest clutch-test-count-total-ensures-connection-before-query ()
  "COUNT should use the shared reconnect path before querying."
  (with-temp-buffer
    (let (ensured captured-conn)
      (setq-local clutch-connection 'stale-conn
                  clutch--base-query "SELECT * FROM t"
                  clutch--result-server-rewritable t)
      (cl-letf (((symbol-function 'clutch--ensure-connection)
                 (lambda ()
                   (setq ensured t)
                   (setq-local clutch-connection 'new-conn)))
                ((symbol-function 'clutch-db-build-count-sql)
                 (lambda (_conn _sql) "SELECT COUNT(*)"))
                ((symbol-function 'clutch-db-query)
                 (lambda (conn _sql)
                   (setq captured-conn conn)
                   (make-clutch-db-result :rows '((3)))))
                ((symbol-function 'clutch--refresh-footer-line) #'ignore)
                ((symbol-function 'message) #'ignore))
        (clutch-result-count-total)
        (should ensured)
        (should (eq captured-conn 'new-conn))
        (should (= clutch--page-total-rows 3))))))

(ert-deftest clutch-test-result-rerun-uses-effective-filtered-query ()
  "Rerun should preserve the current WHERE filter."
  (with-temp-buffer
    (let (executed)
      (setq-local clutch-connection 'fake-conn
                  clutch--base-query "SELECT * FROM t"
                  clutch--where-filter "id = 1")
      (cl-letf (((symbol-function 'clutch-db-apply-where)
                 (lambda (_conn sql filter)
                   (format "FILTER[%s]{%s}" filter sql)))
                ((symbol-function 'clutch--execute)
                 (lambda (sql &optional conn)
                   (setq executed (list sql conn)))))
        (clutch-result-rerun)
        (should (equal executed
                       '("FILTER[id = 1]{SELECT * FROM t}" nil)))))))

(ert-deftest clutch-test-preview-execution-sql-uses-effective-filtered-query ()
  "Preview should show the filtered SQL in result mode."
  (with-temp-buffer
    (clutch-result-mode)
    (let (captured)
      (setq-local clutch--base-query "SELECT * FROM t"
                  clutch--where-filter "id = 1")
      (cl-letf (((symbol-function 'clutch-db-apply-where)
                 (lambda (_conn sql filter)
                   (format "FILTER[%s]{%s}" filter sql)))
                ((symbol-function 'clutch--preview-sql-buffer)
                 (lambda (sql)
                   (setq captured sql))))
        (clutch-preview-execution-sql)
        (should (equal captured "FILTER[id = 1]{SELECT * FROM t}"))))))

(ert-deftest clutch-test-preview-execution-sql-prefers-semicolon-statement-bounds-in-sql-buffer ()
  "Preview should mirror DWIM statement bounds for semicolon-delimited SQL buffers."
  (with-temp-buffer
    (insert "INSERT INTO demo(note) VALUES (E'first line\n\nthird line');\n\nSELECT 2")
    (goto-char (point-min))
    (search-forward "third")
    (let (captured)
      (cl-letf (((symbol-function 'clutch--preview-sql-buffer)
                 (lambda (sql) (setq captured sql))))
        (clutch-preview-execution-sql)
        (should (equal captured
                       "INSERT INTO demo(note) VALUES (E'first line\n\nthird line')"))))))

(ert-deftest clutch-test-execute-params-fallback-renders-sql-before-query ()
  "Fallback parameter execution should render SQL via escape helpers."
  (let (captured-sql)
    (cl-letf (((symbol-function 'clutch-db-escape-literal)
               (lambda (_conn value)
                 (format "'%s'" value)))
              ((symbol-function 'clutch-db-query)
               (lambda (_conn sql)
                 (setq captured-sql sql)
                 'ok)))
      (should (eq (clutch-db-execute-params
                   'fake-conn
                   "UPDATE demo SET name = ?, age = ? WHERE note IS ?"
                   '("alice" 7 nil))
                  'ok))
      (should (equal captured-sql
                     "UPDATE demo SET name = 'alice', age = 7 WHERE note IS NULL")))))

(ert-deftest clutch-test-execute-params-fallback-json-error-surfaces ()
  "Fallback parameter execution should signal when JSON parameter serialization fails."
  (let ((payload (make-hash-table :test 'equal))
        query-called)
    (puthash "key" "value" payload)
    (cl-letf (((symbol-function 'json-serialize)
               (lambda (_value)
                 (signal 'wrong-type-argument '("json serialization failed"))))
              ((symbol-function 'clutch-db-query)
               (lambda (&rest _args)
                 (setq query-called t)
                 'unexpected)))
      (let ((err (should-error
                  (clutch-db-execute-params
                   'fake-conn
                   "INSERT INTO demo(payload) VALUES (?)"
                   (list payload))
                  :type 'clutch-db-error)))
        (should-not query-called)
        (should (string-match-p
                 "Cannot serialize parameter value as JSON"
                 (cadr err)))))))

(ert-deftest clutch-test-execute-statements-remembers-error-details ()
  "Batch statement failures should store details for early and final errors."
  (dolist (case '((("INSERT INTO first VALUES (1)"
                    "INSERT INTO second VALUES (2)")
                   "INSERT INTO first VALUES (1)"
                   1)
                  (("INSERT INTO ok_rows VALUES (1)"
                    "INSERT INTO broken_rows VALUES (2)")
                   "INSERT INTO broken_rows VALUES (2)"
                   2)))
    (pcase-let ((`(,stmts ,broken-sql ,statement-index) case))
      (with-temp-buffer
        (let ((clutch-debug-mode t)
              (raw-message "Connection refused (host=db.example.com, port=3306)")
              displayed)
          (setq-local clutch-connection 'fake-conn)
          (cl-letf (((symbol-function 'clutch--backend-key-from-conn)
                     (lambda (_conn) 'mysql))
                    ((symbol-function 'clutch-result--display-error)
                     (lambda (_conn sql summary message &optional _elapsed hint)
                       (setq displayed (list sql summary message hint))
                       (current-buffer)))
                    ((symbol-function 'clutch--run-db-query)
                     (lambda (_conn sql)
                       (if (equal sql broken-sql)
                           (signal 'clutch-db-error (list raw-message))
                         'ok))))
            (let* ((display-parts (clutch--humanize-db-error-parts raw-message))
                   (result-summary (plist-get display-parts :summary))
                   (result-hint (plist-get display-parts :hint))
                   (display-summary
                    (condition-case err
                        (signal 'clutch-db-error (list raw-message))
                      (clutch-db-error
                       (clutch--humanize-db-error (error-message-string err)))))
                   (signaled (should-error (clutch--execute-statements stmts)
                                           :type 'user-error)))
              (should (equal (cadr signaled)
                             (format "Statement %d failed: %s"
                                     statement-index
                                     (clutch--debug-workflow-message
                                      display-summary))))
              (should (equal displayed
                             (list broken-sql
                                   result-summary
                                   raw-message
                                   result-hint)))
              (let* ((details clutch--buffer-error-details)
                     (diag (plist-get details :diag))
                     (event (car clutch--debug-events)))
                (should details)
                (should (eq (plist-get details :backend) 'mysql))
                (should (equal (plist-get diag :raw-message) raw-message))
                (should (equal (plist-get (plist-get diag :context) :sql)
                               broken-sql))
                (should event)
                (should (equal (plist-get event :phase) "error"))
                (should (equal (plist-get event :summary)
                               display-summary))))))))))

(ert-deftest clutch-test-abort-execution-error-renders-result-without-message ()
  "Single-statement execution errors should not duplicate details in messages."
  (with-temp-buffer
    (insert "SELECT * FROM missing_users")
    (let ((raw-message "Table 'demo.missing_users' doesn't exist")
          err displayed messages)
      (condition-case caught
          (signal 'clutch-db-error (list raw-message))
        (clutch-db-error
         (setq err caught)))
      (cl-letf (((symbol-function 'clutch-result--display-error)
                 (lambda (_conn sql summary message &optional elapsed hint)
                   (setq displayed (list sql summary message elapsed hint))
                   (current-buffer)))
                ((symbol-function 'message)
                 (lambda (fmt &rest args)
                   (push (apply #'format fmt args) messages))))
        (let ((clutch--executing-sql-start (point-min))
              (clutch--executing-sql-end (point-max)))
          (catch 'clutch--execution-aborted
            (clutch--abort-execution-on-db-error
             (current-buffer) 'fake-conn "SELECT * FROM missing_users" err 0.012)))
        (should displayed)
        (should (equal (car displayed) "SELECT * FROM missing_users"))
        (should-not messages)
        (should (overlayp clutch--executed-sql-overlay))
        (should (string-match-p
                 "Last failed SQL"
                 (overlay-get clutch--executed-sql-overlay 'help-echo)))
        (let* ((before (overlay-get clutch--executed-sql-overlay 'before-string))
               (display (get-text-property 0 'display before)))
          (if display
              (should (equal display
                             '(left-fringe clutch-executed-sql-dot
                                           clutch-failed-sql-marker-face)))
            (should (eq (get-text-property 0 'face before)
                        'clutch-failed-sql-marker-face))))
        (should (eq clutch--last-result-buffer (current-buffer)))))))

(ert-deftest clutch-test-display-error-result-renders-result-buffer ()
  "SQL errors should render in the result buffer without source overlays."
  (let ((source (generate-new-buffer " *clutch-error-source*"))
        shown result-buf)
    (unwind-protect
        (with-current-buffer source
          (setq-local clutch-connection 'fake-conn
                      clutch--connection-params '(:backend oracle :database "db")
                      clutch--conn-sql-product 'oracle)
          (insert "SELECT missing_col FROM dual")
          (cl-letf (((symbol-function 'clutch--connection-alive-p)
                     (lambda (_conn) nil))
                    ((symbol-function 'clutch-result--show-buffer)
                     (lambda (buf) (setq shown buf) buf)))
            (setq result-buf
                  (clutch-result--display-error
                   clutch-connection
                   (buffer-string)
                   "ORA-00904: invalid column name"
                   "ORA-00904: \"MISSING_COL\": invalid identifier"
                   0.012
                   "invalid column name")))
          (should (eq shown result-buf))
          (should-not (overlays-in (point-min) (point-max)))
          (with-current-buffer result-buf
            (should (derived-mode-p 'clutch-result-mode))
            (should (equal clutch--connection-params
                           '(:backend oracle :database "db")))
            (should-not truncate-lines)
            (should word-wrap)
            (let ((text (buffer-string)))
              (should-not (string-match-p "\\`ERROR\n" text))
              (should (string-match-p "Hint: invalid column name" text))
              (should (string-match-p "invalid column name" text))
              (should (string-match-p "ORA-00904" text))
              (should-not (string-match-p "Details" text))
              (should-not (string-match-p "SELECT missing_col FROM dual" text))
              (should (string-match-p "Failed in" text)))))
      (when (buffer-live-p result-buf)
        (kill-buffer result-buf))
      (when (buffer-live-p source)
        (kill-buffer source)))))

(ert-deftest clutch-test-execute-and-mark-skips-success-overlay-on-error ()
  "Failed execution should not mark SQL as successfully executed."
  (with-temp-buffer
    (insert "SELECT bad_col FROM dual")
    (let ((marked nil))
      (cl-letf (((symbol-function 'clutch--execute) (lambda (&rest _) nil))
                ((symbol-function 'clutch--mark-executed-sql-region)
                 (lambda (&rest _) (setq marked t))))
        (clutch--execute-and-mark (buffer-string) (point-min) (point-max))
        (should-not marked)))))

(ert-deftest clutch-test-executed-sql-overlay-marks-statement-start-line ()
  "Executed SQL should use a start-line marker instead of a body highlight."
  (with-temp-buffer
    (insert "  SELECT 1;\n  SELECT 2;")
    (clutch--mark-executed-sql-region (point-min) (point-max))
    (should (overlayp clutch--executed-sql-overlay))
    (should (= (overlay-start clutch--executed-sql-overlay) (point-min)))
    (let* ((before (overlay-get clutch--executed-sql-overlay 'before-string))
           (display (get-text-property 0 'display before)))
      (if display
          (should (equal display
                         '(left-fringe clutch-executed-sql-dot
                                       clutch-executed-sql-marker-face)))
        (should (eq (get-text-property 0 'face before)
                    'clutch-executed-sql-marker-face))))
    (should-not (overlay-get clutch--executed-sql-overlay 'modification-hooks))))

(ert-deftest clutch-test-execute-quit-disconnects-and-clears-connection ()
  "Quit should abandon the connection when no backend interrupt is available."
  (with-temp-buffer
    (let ((buf (current-buffer))
          (disconnected nil)
          (clutch--tx-dirty-cache (make-hash-table :test 'eq))
          (clutch-connection 'fake-conn)
          (clutch--executing-p nil))
      (puthash clutch-connection t clutch--tx-dirty-cache)
      (cl-letf (((symbol-function 'clutch--ensure-connection) (lambda () t))
                ((symbol-function 'clutch-result--check-pending-changes) #'ignore)
                ((symbol-function 'clutch-db-sql-destructive-p) (lambda (_sql) nil))
                ((symbol-function 'clutch--update-mode-line) (lambda () nil))
                ((symbol-function 'clutch-db-sql-select-query-p) (lambda (_sql) t))
                ((symbol-function 'clutch--execute-select) (lambda (&rest _args) (signal 'quit nil)))
                ((symbol-function 'clutch--connection-alive-p) (lambda (_conn) t))
                ((symbol-function 'clutch-db-disconnect)
                 (lambda (_conn) (setq disconnected t))))
        (should-error (clutch--execute "SELECT 1" clutch-connection)
                      :type 'user-error)
        (should disconnected)
        (with-current-buffer buf
          (should-not clutch-connection))
        (should-not (gethash 'fake-conn clutch--tx-dirty-cache))
        (should-not clutch--executing-p)))))

(ert-deftest clutch-test-execute-starts-spinner-when-query-begins ()
  "Executing a query should start the global spinner."
  (with-temp-buffer
    (let ((clutch-connection 'fake-conn)
          (clutch--executing-p nil)
          spinner-started
          execute-saw-spinner)
      (cl-letf (((symbol-function 'clutch--ensure-connection) #'ignore)
                ((symbol-function 'clutch-result--check-pending-changes) #'ignore)
                ((symbol-function 'clutch-db-sql-destructive-p) (lambda (_sql) nil))
                ((symbol-function 'clutch--require-risky-dml-confirmation) #'ignore)
                ((symbol-function 'clutch--spinner-start)
                 (lambda () (setq spinner-started t)))
                ((symbol-function 'clutch--update-mode-line) #'ignore)
                ((symbol-function 'redisplay) #'ignore)
                ((symbol-function 'clutch-db-sql-select-query-p) (lambda (_sql) t))
                ((symbol-function 'clutch--execute-select)
                 (lambda (&rest _args)
                   (setq execute-saw-spinner spinner-started))))
        (clutch--execute "SELECT 1" clutch-connection)
        (should spinner-started)
        (should execute-saw-spinner)
        (should-not clutch--executing-p)))))

(ert-deftest clutch-test-execute-in-result-buffer-keeps-table-header-line ()
  "Executing from a result buffer should not replace the table header line."
  (with-temp-buffer
    (clutch-result-mode)
    (let ((clutch-connection 'fake-conn)
          (clutch--executing-p nil)
          (header-line-format " result header")
          seen-header)
      (cl-letf (((symbol-function 'clutch--ensure-connection) #'ignore)
                ((symbol-function 'clutch-result--check-pending-changes) #'ignore)
                ((symbol-function 'clutch-db-sql-destructive-p) (lambda (_sql) nil))
                ((symbol-function 'clutch--require-risky-dml-confirmation) #'ignore)
                ((symbol-function 'clutch--spinner-start) #'ignore)
                ((symbol-function 'redisplay) #'ignore)
                ((symbol-function 'clutch-db-sql-select-query-p) (lambda (_sql) t))
                ((symbol-function 'clutch--execute-select)
                 (lambda (&rest _args)
                   (setq seen-header header-line-format))))
        (clutch--execute "SELECT 1" clutch-connection)
        (should (equal seen-header " result header"))
        (should (equal header-line-format " result header"))))))

(ert-deftest clutch-test-execute-quit-prefers-backend-interrupt-over-disconnect ()
  "Quit should keep the session when a backend interrupt succeeds."
  (with-temp-buffer
    (let* ((buf (current-buffer))
           (conn 'fake-conn)
           (interrupted nil)
           (disconnected nil)
           (clutch--tx-dirty-cache (make-hash-table :test 'eq))
           (clutch-connection conn)
           (clutch--executing-p nil))
      (cl-letf (((symbol-function 'clutch--ensure-connection) (lambda () t))
                ((symbol-function 'clutch-result--check-pending-changes) #'ignore)
                ((symbol-function 'clutch-db-sql-destructive-p) (lambda (_sql) nil))
                ((symbol-function 'clutch--update-mode-line) (lambda () nil))
                ((symbol-function 'clutch-db-sql-select-query-p) (lambda (_sql) t))
                ((symbol-function 'clutch--execute-select) (lambda (&rest _args) (signal 'quit nil)))
                ((symbol-function 'clutch--connection-alive-p) (lambda (_conn) t))
                ((symbol-function 'clutch-db-interrupt-query)
                 (lambda (_conn)
                   (setq interrupted t)
                   t))
                ((symbol-function 'clutch-db-disconnect)
                 (lambda (_conn) (setq disconnected t))))
        (should-error (clutch--execute "SELECT pg_sleep(10)" clutch-connection)
                      :type 'user-error)
        (should interrupted)
        (should-not disconnected)
        (with-current-buffer buf
          (should (eq clutch-connection conn)))
        (should-not clutch--executing-p)))))

(ert-deftest clutch-test-handle-query-quit-remembers-interrupt-error-details-and-debug-event ()
  "Interrupt RPC failures should store details and a cancel debug event."
  (with-temp-buffer
    (let* ((clutch-debug-mode t)
           (clutch-connection 'fake-conn)
           (raw-message "Connection refused (host=db.example.com, port=3306)")
           (captured-message nil)
           (disconnected nil))
      (cl-letf (((symbol-function 'clutch--backend-key-from-conn)
                 (lambda (_conn) 'pg))
                ((symbol-function 'clutch--connection-alive-p)
                 (lambda (_conn) t))
                ((symbol-function 'clutch-db-interrupt-query)
                 (lambda (_conn)
                   (signal 'clutch-db-error (list raw-message))))
                ((symbol-function 'clutch-db-disconnect)
                 (lambda (_conn)
                   (setq disconnected t)))
                ((symbol-function 'message)
                 (lambda (fmt &rest args)
                   (setq captured-message (apply #'format fmt args)))))
        (should-error (clutch--handle-query-quit clutch-connection)
                      :type 'clutch-query-interrupted)
        (let* ((summary (clutch--humanize-db-error raw-message))
               (message-summary
                (condition-case err
                    (signal 'clutch-db-error (list raw-message))
                  (clutch-db-error
                   (clutch--humanize-db-error (error-message-string err)))))
               (details clutch--buffer-error-details)
               (diag (plist-get details :diag))
               (context (plist-get diag :context))
               (cancel-event
                (cl-find-if
                 (lambda (event)
                   (and (equal (plist-get event :op) "cancel")
                        (equal (plist-get event :phase) "error")))
                 clutch--debug-events))
               (interrupt-event
                (cl-find-if
                 (lambda (event)
                   (and (equal (plist-get event :op) "interrupt")
                        (equal (plist-get event :phase) "disconnect")))
                 clutch--debug-events)))
          (should disconnected)
          (should details)
          (should (eq (plist-get details :backend) 'pg))
          (should (equal (plist-get details :summary) summary))
          (should (equal (plist-get diag :raw-message) raw-message))
          (should (plist-member context :sql))
          (should-not (plist-get context :sql))
          (should (equal captured-message
                         (format "Interrupt failed: %s"
                                 (clutch--debug-workflow-message message-summary))))
          (should cancel-event)
          (should (equal (plist-get cancel-event :summary) message-summary))
          (should interrupt-event))))))

(ert-deftest clutch-test-execute-runs-risky-dml-confirmation ()
  "Execute should run risky DML confirmation before dispatch."
  (let ((called nil)
        (clutch-connection 'fake-conn))
    (cl-letf (((symbol-function 'clutch--ensure-connection) (lambda () t))
              ((symbol-function 'clutch-result--check-pending-changes) #'ignore)
              ((symbol-function 'clutch-db-sql-destructive-p) (lambda (_sql) nil))
              ((symbol-function 'clutch--require-risky-dml-confirmation)
               (lambda (sql) (setq called sql)))
              ((symbol-function 'clutch--update-mode-line) (lambda () nil))
              ((symbol-function 'clutch-db-sql-select-query-p) (lambda (_sql) t))
              ((symbol-function 'clutch--execute-select) (lambda (&rest _args) 'ok)))
      (clutch--execute "UPDATE users SET x=1" clutch-connection)
      (should (equal called "UPDATE users SET x=1")))))

(ert-deftest clutch-test-execute-from-arbitrary-buffer-uses-live-connection ()
  "`clutch-execute' should execute with a connection found in another buffer."
  (let ((conn-buf (generate-new-buffer " *clutch-conn*"))
        captured-conn)
    (unwind-protect
        (progn
          (with-current-buffer conn-buf
            (setq-local clutch-connection 'fake-conn))
          (with-temp-buffer
            (insert "SELECT 1")
            (cl-letf (((symbol-function 'use-region-p) (lambda () nil))
                      ((symbol-function 'clutch--connection-alive-p)
                       (lambda (conn) (eq conn 'fake-conn)))
                      ((symbol-function 'clutch--try-reconnect) (lambda () nil))
                      ((symbol-function 'clutch-result--check-pending-changes) #'ignore)
                      ((symbol-function 'clutch-db-sql-destructive-p)
                       (lambda (_sql) nil))
                      ((symbol-function 'clutch--require-risky-dml-confirmation)
                       #'ignore)
                      ((symbol-function 'clutch--spinner-start) #'ignore)
                      ((symbol-function 'clutch--update-mode-line) #'ignore)
                      ((symbol-function 'clutch-db-sql-select-query-p) (lambda (_sql) t))
                      ((symbol-function 'clutch--execute-select)
                       (lambda (_sql conn &optional _result-context)
                         (setq captured-conn conn)
                         'ok))
                      ((symbol-function 'clutch--mark-executed-sql-region)
                       #'ignore))
              (clutch-execute "SELECT 1")
              (should (eq captured-conn 'fake-conn)))))
      (when (buffer-live-p conn-buf)
        (kill-buffer conn-buf)))))

(ert-deftest clutch-test-execute-statements-confirms-each-nonselect ()
  "Batch execution should apply destructive and risky DML guards."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn)
    (let (queries risky-sqls yes-prompts)
      (cl-letf (((symbol-function 'clutch--run-db-query)
                 (lambda (_conn sql)
                   (push sql queries)
                   (make-clutch-db-result :affected-rows 1)))
                ((symbol-function 'clutch--require-risky-dml-confirmation)
                 (lambda (sql) (push sql risky-sqls)))
                ((symbol-function 'clutch-db-sql-schema-affecting-p)
                 (lambda (_sql) nil))
                ((symbol-function 'yes-or-no-p)
                 (lambda (_prompt)
                   (setq yes-prompts (1+ (or yes-prompts 0)))
                   t))
                ((symbol-function 'message) #'ignore))
        (clutch--execute-statements
         '("DROP TABLE users" "UPDATE users SET admin = 1"))
        (should (equal (nreverse queries)
                       '("DROP TABLE users" "UPDATE users SET admin = 1")))
        (should (equal (nreverse risky-sqls)
                       '("DROP TABLE users" "UPDATE users SET admin = 1")))
        (should (= yes-prompts 1))))))

(ert-deftest clutch-test-execute-statements-refreshes-schema-after-ddl ()
  "Batch DDL execution should refresh or invalidate schema metadata."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn)
    (let (refreshed)
      (cl-letf (((symbol-function 'clutch--run-db-query)
                 (lambda (_conn _sql)
                   (make-clutch-db-result :affected-rows 0)))
                ((symbol-function 'clutch-db-sql-schema-affecting-p)
                 (lambda (_sql) t))
                ((symbol-function 'clutch-db-eager-schema-refresh-p)
                 (lambda (_conn) nil))
                ((symbol-function 'clutch--refresh-schema-cache-async)
                 (lambda (conn) (setq refreshed conn)))
                ((symbol-function 'clutch--require-risky-dml-confirmation)
                 #'ignore)
                ((symbol-function 'yes-or-no-p) (lambda (_prompt) t))
                ((symbol-function 'message) #'ignore))
        (clutch--execute-statements '("CREATE TABLE users (id INT)"))
        (should (eq refreshed 'fake-conn))))))

;;;; Temporal formatting

(ert-deftest clutch-test-format-temporal-values ()
  "Clutch-db-format-temporal should format supported temporal plists."
  (dolist (case '((datetime
                   (:year 2024 :month 1 :day 15
                    :hours 13 :minutes 45 :seconds 30)
                   "2024-01-15 13:45:30")
                  (date-only
                   (:year 2024 :month 6 :day 1)
                   "2024-06-01")
                  (time-only
                   (:hours 13 :minutes 5 :seconds 0 :negative nil)
                   "13:05:00")
                  (negative-time
                   (:hours 1 :minutes 0 :seconds 0 :negative t)
                   "-01:00:00")
                  (non-temporal
                   (:foo 1 :bar 2)
                   nil)))
    (pcase-let ((`(,label ,value ,expected) case))
      (ert-info ((format "case: %s" label))
        (should (equal (clutch-db-format-temporal value) expected))))))

;;;; Live integration tests

(defun clutch-test--live-connect-params ()
  "Return connection params for `clutch-test--with-conn'."
  (let ((params (if clutch-test-url
                    (list :url clutch-test-url)
                  (list :host clutch-test-host
                        :port clutch-test-port
                        :database clutch-test-database))))
    (when clutch-test-user
      (setq params (plist-put params :user clutch-test-user)))
    (when clutch-test-password
      (setq params (plist-put params :password clutch-test-password)))
    (when clutch-test-display-name
      (setq params (plist-put params :display-name clutch-test-display-name)))
    (when clutch-test-props
      (setq params (plist-put params :props clutch-test-props)))
    params))

(defun clutch-test--duckdb-live-p ()
  "Return non-nil when live tests target DuckDB through generic JDBC."
  (and (eq clutch-test-backend 'jdbc)
       (stringp clutch-test-url)
       (string-match-p "\\`jdbc:duckdb:" clutch-test-url)))

(defun clutch-test--clickhouse-live-p ()
  "Return non-nil when live tests target ClickHouse."
  (eq clutch-test-backend 'clickhouse))

(defun clutch-test--live-name-member-p (name names)
  "Return non-nil when NAME appears in NAMES, ignoring metadata case."
  (cl-find name names :test #'string-equal-ignore-case))

(defun clutch-test--updateable-live-backend-p ()
  "Return non-nil when generic live workflow SQL is valid for the backend."
  (or (memq clutch-test-backend '(mysql pg sqlserver oracle))
      (clutch-test--duckdb-live-p)))

(defun clutch-test--result-live-backend-p ()
  "Return non-nil when result workflow SQL is valid for the backend."
  (or (clutch-test--updateable-live-backend-p)
      (clutch-test--clickhouse-live-p)))

(defun clutch-test--live-column-type (kind)
  "Return a live-test SQL column type for KIND."
  (pcase kind
    ('int (if (clutch-test--clickhouse-live-p) "Int32" "INT"))
    ('string (if (clutch-test--clickhouse-live-p) "String" "VARCHAR(64)"))
    (_ (error "Unknown live test column kind: %S" kind))))

(defun clutch-test--live-create-table-sql (table columns)
  "Return CREATE TABLE SQL for TABLE with COLUMNS.
COLUMNS entries have the shape (NAME KIND . ATTRS)."
  (format "CREATE TABLE %s (%s)%s"
          table
          (mapconcat
           (lambda (column)
             (pcase-let ((`(,name ,kind . ,attrs) column))
               (format "%s %s%s"
                       name
                       (clutch-test--live-column-type kind)
                       (if (and (memq 'primary attrs)
                                (not (clutch-test--clickhouse-live-p)))
                           " PRIMARY KEY"
                         ""))))
           columns
           ", ")
          (if (clutch-test--clickhouse-live-p) " ENGINE = Memory" "")))

(defun clutch-test--live-row-prefix-strings (row count)
  "Return the first COUNT values from ROW formatted as strings."
  (mapcar (lambda (value) (format "%s" value))
          (seq-take row count)))

(defun clutch-test--live-row-ids (rows)
  "Return the first-column identifiers from live ROWS as numbers."
  (mapcar (lambda (row) (string-to-number (format "%s" (car row))))
          rows))

(defmacro clutch-test--with-conn (var &rest body)
  "Execute BODY with VAR bound to a live connection.
Skips if neither `clutch-test-password' nor `clutch-test-url' is set."
  (declare (indent 1))
  `(if (and (null clutch-test-password)
            (null clutch-test-url))
       (ert-skip "Set clutch-test-password or clutch-test-url to enable live tests")
     (let ((mysql-tls-verify-server nil))
       (let ((,var (clutch-db-connect
                    clutch-test-backend
                    (clutch-test--live-connect-params))))
         (unwind-protect
             (progn ,@body)
           (clutch-db-disconnect ,var))))))

(defmacro clutch-test--with-live-result-buffer (name &rest body)
  "Run BODY with live SELECT results isolated to result buffer NAME."
  (declare (indent 1) (debug (form body)))
  `(let ((clutch-test--result-name ,name))
     (cl-letf (((symbol-function 'clutch-result--buffer-name)
                (lambda () clutch-test--result-name)))
       (unwind-protect
           (progn ,@body)
         (when-let* ((buf (get-buffer clutch-test--result-name)))
           (kill-buffer buf))))))

(defun clutch-test--execute-live-select (conn sql)
  "Execute SQL through the result UI path for live connection CONN."
  (with-temp-buffer
    (let ((clutch-connection conn)
          (clutch--source-window (selected-window)))
      (clutch--execute-select sql conn))))

(ert-deftest clutch-test-live-schema-introspection ()
  :tags '(:clutch-live)
  "Test schema introspection functions."
  (clutch-test--with-conn conn
    (let* ((table (format "clutch_schema_%d" (emacs-pid)))
           (drop-sql (format "DROP TABLE IF EXISTS %s" table))
           (create-sql
            (clutch-test--live-create-table-sql
             table '((id int primary) (name string)))))
      (unwind-protect
          (progn
            (clutch-db-query conn drop-sql)
	      (clutch-db-query conn create-sql)
	      (let ((tables (clutch-db-list-tables conn)))
	        (should (listp tables))
	        (should (clutch-test--live-name-member-p table tables)))
	      (let ((columns (clutch-db-list-columns conn table)))
	        (should (listp columns))
	        (should (clutch-test--live-name-member-p "id" columns))
	        (should (clutch-test--live-name-member-p "name" columns)))
	      (let ((pk-cols (clutch-db-primary-key-columns conn table)))
	        (unless (clutch-test--clickhouse-live-p)
	          (should (equal (mapcar #'downcase pk-cols) '("id"))))))
        (ignore-errors (clutch-db-query conn drop-sql))))))

(ert-deftest clutch-test-live-object-describe-uses-real-table-and-index-metadata ()
  :tags '(:clutch-live)
  "Object describe should render real table/index metadata from the backend."
  (unless (memq clutch-test-backend '(mysql pg))
    (ert-skip "Object describe live test currently covers MySQL/PostgreSQL"))
  (clutch-test--with-conn conn
    (let* ((table (format "clutch_obj_desc_%d" (emacs-pid)))
           (index (format "idx_clutch_obj_desc_%d" (emacs-pid)))
           (drop-sql (format "DROP TABLE IF EXISTS %s" table))
           (create-sql
            (clutch-test--live-create-table-sql
             table '((id int primary) (name string))))
           (index-sql
            (format "CREATE INDEX %s ON %s (name)" index table)))
      (let ((clutch--object-cache (make-hash-table :test 'equal))
            (clutch--object-warmup-timers (make-hash-table :test 'equal))
            (clutch--object-warmup-generations (make-hash-table :test 'equal))
            (clutch--column-details-cache (make-hash-table :test 'equal))
            (clutch--column-details-status-cache (make-hash-table :test 'equal)))
        (unwind-protect
            (progn
              (clutch-db-query conn drop-sql)
              (clutch-db-query conn create-sql)
              (clutch-db-query conn index-sql)
              (let* ((table-entry
                      (cl-find table
                               (clutch-db-list-table-entries conn)
                               :key (lambda (entry) (plist-get entry :name))
                               :test #'string=))
                     (_warmed-indexes
                      (clutch--object-type-entries conn "INDEX" t))
                     (index-entry
                      (cl-find index
                               (clutch-db-list-objects conn 'indexes)
                               :key (lambda (entry) (plist-get entry :name))
                               :test #'string=)))
                (should table-entry)
                (should index-entry)
                (let ((text (clutch--object-describe-text conn table-entry)))
                  (should (string-match-p (regexp-quote table) text))
                  (should (string-match-p "^Columns (2)$" text))
                  (should (string-match-p "^  id\\_>" text))
                  (should (string-match-p "^  name\\_>" text))
                  (should (string-match-p (regexp-quote index) text)))
                (let ((text (clutch--object-describe-text conn index-entry)))
                  (should (string-match-p (regexp-quote index) text))
                  (should (string-match-p "^Columns (1)$" text))
                  (should (string-match-p "^  name\\_>" text)))))
          (ignore-errors (clutch-db-query conn drop-sql)))))))

(ert-deftest clutch-test-live-paged-sql-building ()
  :tags '(:clutch-live)
  "Test paged SQL query building."
  (clutch-test--with-conn conn
    (let* ((table (format "clutch_paged_%d" (emacs-pid)))
           (drop-sql (format "DROP TABLE IF EXISTS %s" table))
           (create-sql
            (clutch-test--live-create-table-sql
             table '((id int primary) (name string))))
           (insert-sql
            (format "INSERT INTO %s (id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c')"
                    table))
           (base-sql (format "SELECT id, name FROM %s" table)))
      (unwind-protect
          (progn
            (clutch-db-query conn drop-sql)
            (clutch-db-query conn create-sql)
            (clutch-db-query conn insert-sql)
	      (let* ((paged (clutch-db-build-paged-sql conn base-sql 0 2))
	             (rows (clutch-db-result-rows
	                    (clutch-db-query conn paged))))
	        (let ((paged-upper (upcase paged)))
	          (should (or (string-match-p "LIMIT" paged-upper)
	                      (string-match-p "OFFSET" paged-upper)
	                      (string-match-p "ROWNUM" paged-upper)
	                      (string-match-p "FETCH" paged-upper))))
	        (should (= (length rows) 2)))
	      (let* ((sort-column (if (eq clutch-test-backend 'oracle) "ID" "id"))
	             (paged (clutch-db-build-paged-sql
	                     conn base-sql 0 2 (cons sort-column "DESC")))
	             (rows (clutch-db-result-rows
	                    (clutch-db-query conn paged))))
	        (should (string-match-p "ORDER BY" paged))
	        (should (equal (mapcar (lambda (row)
	                                 (list (format "%s" (car row)) (cadr row)))
	                               rows)
	                       '(("3" "c") ("2" "b"))))))
        (ignore-errors (clutch-db-query conn drop-sql))))))

(ert-deftest clutch-test-live-result-filter-sort-page-count-export-workflow ()
  :tags '(:clutch-live)
  "Result buffer workflows should run real backend queries end-to-end."
  (unless (clutch-test--result-live-backend-p)
    (ert-skip "Result workflow live test covers MySQL/PostgreSQL/SQL Server/Oracle/ClickHouse/DuckDB"))
  (clutch-test--with-conn conn
    (let* ((table (format "clutch_result_flow_%d" (emacs-pid)))
           (drop-sql (format "DROP TABLE IF EXISTS %s" table))
           (create-sql
            (clutch-test--live-create-table-sql
             table '((id int primary) (name string) (score int))))
           (insert-sql
            (format
             "INSERT INTO %s (id, name, score) VALUES (1, 'ann', 10), (2, 'bob', 20), (3, 'cam', 30), (4, 'dan', 40), (5, 'eve', 50)"
             table))
           (select-sql (format "SELECT id, name, score FROM %s ORDER BY id" table))
           (result-name (format " *clutch-flow-live-%d*" (emacs-pid))))
      (unwind-protect
          (progn
            (clutch-db-query conn drop-sql)
            (clutch-db-query conn create-sql)
            (clutch-db-query conn insert-sql)
            (clutch-test--with-live-result-buffer result-name
              (let ((clutch-result-max-rows 2))
                (clutch-test--execute-live-select conn select-sql))
              (with-current-buffer result-name
                (set-window-buffer (selected-window) (current-buffer))
                (setq-local clutch-result-max-rows 2)
                (should (equal (clutch-test--live-row-ids clutch--result-rows)
                               '(1 2)))
                (should (string-match-p "ann" (buffer-string)))
                (should clutch--page-has-more)
                (cl-letf (((symbol-function 'message) #'ignore))
                  (clutch-result-count-total)
                  (should (= clutch--page-total-rows 5))
                  (clutch-result-last-page)
                  (should (= clutch--page-current 2))
                  (should (= clutch--page-offset 3))
                  (should-not clutch--page-has-more)
                  (should (equal (clutch-test--live-row-ids clutch--result-rows)
                                 '(4 5)))
                  (should (string-match-p "eve" (buffer-string)))
                  (let ((score-column
                         (cl-find "score" clutch--result-columns
                                  :test #'string-equal-ignore-case)))
                    (should score-column)
                    (clutch-result--sort score-column t))
                  (should (equal (clutch-test--live-row-ids clutch--result-rows)
                                 '(5 4)))
                  (should (string-match-p "dan" (buffer-string)))
                  (clutch-result-next-page)
                  (should (= clutch--page-current 1))
                  (should clutch--page-has-more)
                  (should (equal (clutch-test--live-row-ids clutch--result-rows)
                                 '(3 2)))
                  (let ((score-column
                         (cl-find "score" clutch--result-columns
                                  :test #'string-equal-ignore-case)))
                    (should score-column)
                    (cl-letf (((symbol-function 'completing-read)
                               (lambda (&rest _args) score-column))
                              ((symbol-function 'read-string)
                               (lambda (&rest _args) "> 20")))
                      (clutch-result-apply-filter))
                    (should (equal clutch--where-filter
                                   (format "%s > 20"
                                           (clutch-db-escape-identifier
                                            conn score-column)))))
                  (should (= clutch--page-current 0))
                  (should (equal (sort (clutch-test--live-row-ids
                                        clutch--result-rows)
                                       #'<)
                                 '(3 4)))
                  (clutch-result-count-total)
                  (should (= clutch--page-total-rows 3))
                  (let ((rows (clutch-result--collect-all-export-rows)))
                    (should (equal (sort (clutch-test--live-row-ids rows) #'<)
                                   '(3 4 5))))))))
        (ignore-errors (clutch-db-query conn drop-sql))))))

(ert-deftest clutch-test-live-mysql-limited-join-duplicate-columns-executes-flat ()
  :tags '(:clutch-live)
  "MySQL limited JOIN results with duplicate column names should not be wrapped."
  (unless (eq clutch-test-backend 'mysql)
    (ert-skip "Duplicate-column derived table restriction is MySQL-specific"))
  (clutch-test--with-conn conn
    (let* ((table-a (format "clutch_dup_a_%d" (emacs-pid)))
           (table-b (format "clutch_dup_b_%d" (emacs-pid)))
           (drop-a (format "DROP TABLE IF EXISTS %s" table-a))
           (drop-b (format "DROP TABLE IF EXISTS %s" table-b))
           (create-a
            (format "CREATE TABLE %s (id INT PRIMARY KEY, name VARCHAR(64))"
                    table-a))
           (create-b
            (format "CREATE TABLE %s (id INT PRIMARY KEY, label VARCHAR(64))"
                    table-b))
           (insert-a
            (format "INSERT INTO %s (id, name) VALUES (1, 'ann'), (2, 'bob')"
                    table-a))
           (insert-b
            (format "INSERT INTO %s (id, label) VALUES (1, 'a1'), (2, 'b2')"
                    table-b))
           (select-sql
            (format "SELECT a.*, b.* FROM %s AS a JOIN %s AS b ON a.id = b.id LIMIT 10"
                    table-a table-b))
           (result-name (format " *clutch-dup-columns-live-%d*" (emacs-pid))))
      (unwind-protect
          (progn
            (clutch-db-query conn drop-b)
            (clutch-db-query conn drop-a)
            (clutch-db-query conn create-a)
            (clutch-db-query conn create-b)
            (clutch-db-query conn insert-a)
            (clutch-db-query conn insert-b)
            (clutch-test--with-live-result-buffer result-name
              (let ((clutch-result-max-rows 1))
                (clutch-test--execute-live-select conn select-sql))
              (with-current-buffer result-name
                (should (equal clutch--result-columns
                               '("id" "name" "id" "label")))
                (should (= (length clutch--result-rows) 2))
                (should-not clutch--page-has-more)
                (should-not clutch--result-server-pageable)
                (should-not clutch--result-server-rewritable)
                (should-not clutch--result-source-table))))
        (ignore-errors (clutch-db-query conn drop-b))
        (ignore-errors (clutch-db-query conn drop-a))))))

(ert-deftest clutch-test-live-pg-ctid-edit-via-execute-select-persists ()
  :tags '(:clutch-live)
  "PostgreSQL no-key edit should work through SELECT row identity injection."
  (unless (eq clutch-test-backend 'pg)
    (ert-skip "CTID row identity is PostgreSQL-specific"))
  (clutch-test--with-conn conn
    (let* ((table (format "clutch_ctid_edit_%d" (emacs-pid)))
           (drop-sql (format "DROP TABLE IF EXISTS %s" table))
           (create-sql (format "CREATE TABLE %s (name TEXT)" table))
           (insert-sql (format "INSERT INTO %s (name) VALUES ('before')" table))
           (select-sql (format "SELECT name FROM %s" table))
           (result-name (format " *clutch-ctid-live-%d*" (emacs-pid))))
      (unwind-protect
          (progn
            (clutch-db-query conn drop-sql)
            (clutch-db-query conn create-sql)
            (clutch-db-query conn insert-sql)
            (clutch-test--with-live-result-buffer result-name
              (clutch-test--execute-live-select conn select-sql)
              (with-current-buffer result-name
                (should (equal (plist-get clutch--row-identity :kind)
                               'row-locator))
                (should (equal (plist-get clutch--row-identity :name) "ctid"))
                (cl-letf (((symbol-function 'yes-or-no-p)
                           (lambda (&rest _) t)))
                  (clutch-result--apply-edit 0 0 "after")
                  (should clutch--pending-edits)
                  (clutch-result-commit)
                  (should-not clutch--pending-edits)
                  (should (equal (caar clutch--result-rows) "after")))))
            (let ((rows (clutch-db-result-rows
                         (clutch-db-query
                          conn
                          (format "SELECT name FROM %s" table)))))
              (should (equal rows '(("after"))))))
        (ignore-errors (clutch-db-query conn drop-sql))))))

(ert-deftest clutch-test-live-pg-ctid-aggregate-select-skips-row-identity-injection ()
  :tags '(:clutch-live)
  "PostgreSQL no-key aggregate SELECT should not receive CTID injection."
  (unless (eq clutch-test-backend 'pg)
    (ert-skip "CTID row identity is PostgreSQL-specific"))
  (clutch-test--with-conn conn
    (let* ((table (format "clutch_ctid_count_%d" (emacs-pid)))
           (drop-sql (format "DROP TABLE IF EXISTS %s" table))
           (create-sql (format "CREATE TABLE %s (name TEXT)" table))
           (insert-sql
            (format "INSERT INTO %s (name) VALUES ('a'), ('b')" table))
           (select-sql (format "SELECT count(1) FROM %s" table))
           (result-name (format " *clutch-ctid-count-live-%d*" (emacs-pid))))
      (unwind-protect
          (progn
            (clutch-db-query conn drop-sql)
            (clutch-db-query conn create-sql)
            (clutch-db-query conn insert-sql)
            (clutch-test--with-live-result-buffer result-name
              (let* ((result (clutch-test--execute-live-select conn select-sql))
                     (rows (clutch-db-result-rows result)))
                (should (equal (format "%s" (caar rows)) "2")))
              (with-current-buffer result-name
                (should (string-match-p "2" (buffer-string))))))
        (ignore-errors (clutch-db-query conn drop-sql))))))

(ert-deftest clutch-test-live-aggregate-select-skips-row-identity-injection ()
  :tags '(:clutch-live)
  "Aggregate SELECT execution should not inject row identity into live SQL."
  (unless (clutch-test--result-live-backend-p)
    (ert-skip "Aggregate row identity live test covers MySQL/PostgreSQL/SQL Server/Oracle/ClickHouse/DuckDB"))
  (clutch-test--with-conn conn
    (let* ((table (format "clutch_issue12_%d" (emacs-pid)))
           (drop-sql (format "DROP TABLE IF EXISTS %s" table))
           (create-sql
            (clutch-test--live-create-table-sql
             table '((id int primary) (name string))))
           (insert-sql
            (format "INSERT INTO %s (id, name) VALUES (1, 'a'), (2, 'b')"
                    table))
           (select-sql (format "SELECT count(1) FROM %s" table))
           (result-name (format " *clutch-count-live-%d*" (emacs-pid))))
      (unwind-protect
          (progn
            (clutch-db-query conn drop-sql)
            (clutch-db-query conn create-sql)
            (clutch-db-query conn insert-sql)
            (clutch-test--with-live-result-buffer result-name
              (let* ((result (clutch-test--execute-live-select conn select-sql))
                     (rows (clutch-db-result-rows result)))
                (should (equal (format "%s" (caar rows)) "2")))
              (with-current-buffer result-name
                (should (string-match-p "2" (buffer-string))))))
        (ignore-errors (clutch-db-query conn drop-sql))))))

(ert-deftest clutch-test-live-edit-field-and-commit-persists ()
  :tags '(:clutch-live)
  "Edit through a real SELECT result and commit the persisted row change."
  (unless (clutch-test--updateable-live-backend-p)
    (ert-skip "Edit live test covers MySQL/PostgreSQL/SQL Server/Oracle/DuckDB"))
  (clutch-test--with-conn conn
    (let* ((table (format "clutch_edit_commit_%d" (emacs-pid)))
           (drop-sql (format "DROP TABLE IF EXISTS %s" table))
           (create-sql
            (clutch-test--live-create-table-sql
             table '((id int primary) (name string))))
           (insert-sql
             (format "INSERT INTO %s (id, name) VALUES (1, 'before')" table))
           (select-sql
            (format "SELECT id, name FROM %s ORDER BY id" table))
           (result-name (format " *clutch-edit-live-%d*" (emacs-pid))))
      (unwind-protect
          (progn
            (clutch-db-query conn drop-sql)
            (clutch-db-query conn create-sql)
            (clutch-db-query conn insert-sql)
            (clutch-test--with-live-result-buffer result-name
              (clutch-test--execute-live-select conn select-sql)
              (with-current-buffer result-name
                (should (equal (clutch-test--live-row-prefix-strings
                                (car clutch--result-rows) 2)
                               '("1" "before")))
                (should (equal (mapcar #'downcase
                                       (plist-get clutch--row-identity :columns))
                               '("id")))
                (cl-letf (((symbol-function 'yes-or-no-p)
                           (lambda (&rest _) t)))
                  (clutch-result--apply-edit 0 1 "after")
                  (should clutch--pending-edits)
                  (clutch-result-commit)
                  (should-not clutch--pending-edits)
                  (should (equal (clutch-test--live-row-prefix-strings
                                  (car clutch--result-rows) 2)
                                 '("1" "after"))))))
            (let* ((res (clutch-db-query conn select-sql))
                   (rows (clutch-db-result-rows res)))
              (should (equal (mapcar (lambda (row)
                                       (clutch-test--live-row-prefix-strings
                                        row 2))
                                     rows)
                             '(("1" "after"))))))
        (ignore-errors (clutch-db-query conn drop-sql))))))

(ert-deftest clutch-test-live-insert-and-delete-commit-persists ()
  :tags '(:clutch-live)
  "Insert and delete staging should persist through real backend commits."
  (unless (clutch-test--updateable-live-backend-p)
    (ert-skip "Insert/delete live test covers MySQL/PostgreSQL/SQL Server/Oracle/DuckDB"))
  (clutch-test--with-conn conn
    (let* ((table (format "clutch_insert_delete_%d" (emacs-pid)))
           (drop-sql (format "DROP TABLE IF EXISTS %s" table))
           (create-sql
            (clutch-test--live-create-table-sql
             table '((id int primary) (name string))))
           (select-sql (format "SELECT id, name FROM %s ORDER BY id" table))
           (result-name (format " *clutch-insert-delete-live-%d*" (emacs-pid)))
           insert-buf)
      (unwind-protect
          (progn
            (clutch-db-query conn drop-sql)
            (clutch-db-query conn create-sql)
            (clutch-test--with-live-result-buffer result-name
              (clutch-test--execute-live-select conn select-sql)
              (with-current-buffer result-name
                (should-not clutch--result-rows)
                (cl-letf (((symbol-function 'pop-to-buffer)
                           (lambda (buf &rest _args)
                             (setq insert-buf buf)
                             buf)))
                  (clutch-result-insert-row)))
              (with-current-buffer insert-buf
                (unless (save-excursion
                          (goto-char (point-min))
                          (re-search-forward "^name.*: " nil t))
                  (cl-letf (((symbol-function 'message) #'ignore))
                    (clutch-result-insert-toggle-field-layout)))
                (goto-char (point-min))
                (should (re-search-forward "^id.*: " nil t))
                (insert "1")
                (goto-char (point-min))
                (should (re-search-forward "^name.*: " nil t))
                (insert "ann")
                (cl-letf (((symbol-function 'quit-window) #'ignore)
                          ((symbol-function 'message) #'ignore))
                  (clutch-result-insert-commit)))
              (with-current-buffer result-name
                (let ((insert (car clutch--pending-inserts)))
                  (should (equal (cdr (assoc-string "id" insert t)) "1"))
                  (should (equal (cdr (assoc-string "name" insert t)) "ann")))
                (cl-letf (((symbol-function 'yes-or-no-p)
                           (lambda (&rest _) t))
                          ((symbol-function 'message) #'ignore))
                  (clutch-result-commit))
                (should-not clutch--pending-inserts))
              (should (equal (mapcar (lambda (row)
                                       (clutch-test--live-row-prefix-strings
                                        row 2))
                                     (clutch-db-result-rows
                                      (clutch-db-query conn select-sql)))
                             '(("1" "ann"))))
              (clutch-test--execute-live-select conn select-sql)
              (with-current-buffer result-name
                (should (equal (clutch-test--live-row-prefix-strings
                                (car clutch--result-rows) 2)
                               '("1" "ann")))
                (cl-letf (((symbol-function 'clutch--selected-row-indices)
                           (lambda () '(0)))
                          ((symbol-function 'message) #'ignore))
                  (clutch-result-delete-rows))
                (should (equal (mapcar (lambda (identity)
                                         (format "%s" (aref identity 0)))
                                       clutch--pending-deletes)
                               '("1")))
                (cl-letf (((symbol-function 'yes-or-no-p)
                           (lambda (&rest _) t))
                          ((symbol-function 'message) #'ignore))
                  (clutch-result-commit))
                (should-not clutch--pending-deletes)))
            (should-not (clutch-db-result-rows
                         (clutch-db-query conn select-sql))))
        (when (buffer-live-p insert-buf)
          (kill-buffer insert-buf))
        (ignore-errors (clutch-db-query conn drop-sql))))))

;;; clutch-test.el ends here
