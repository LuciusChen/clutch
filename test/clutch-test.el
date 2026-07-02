;;; clutch-test.el --- ERT tests for database workflows -*- lexical-binding: t; -*-

;; Author: Lucius Chen <chenyh572@gmail.com>
;; Maintainer: Lucius Chen <chenyh572@gmail.com>
;; URL: https://github.com/LuciusChen/clutch

;;; Commentary:

;; ERT tests for the clutch user interface layer.
;;
;; Unit tests run without a database server.
;; Native live tests cover MySQL, PostgreSQL, MongoDB, and Redis.  The live
;; runner starts or reuses local containers, preferring Podman on Linux and
;; OrbStack-backed Docker on macOS:
;;   ./test/run-native-live-tests.sh
;;
;; Manual live setup:
;;   docker run -d -e MYSQL_ROOT_PASSWORD=test -p 127.0.0.1:55306:3306 mysql:8
;;   docker run -d -e POSTGRES_INITDB_ARGS=--auth-host=md5 -e POSTGRES_PASSWORD=test -p 127.0.0.1:55432:5432 postgres:16 -c password_encryption=md5
;;   docker run -d -p 127.0.0.1:57017:27017 mongo:7
;;   docker run -d -p 127.0.0.1:56379:6379 redis:7-alpine
;;
;; Run unit tests from the repository root:
;;   emacs --batch -Q -L . -L test -L ../mysql.el -L ../pg-el \
;;     --eval '(setq load-prefer-newer t)' \
;;     -l ert -l clutch-test \
;;     -f ert-run-tests-batch-and-exit

;;; Code:

(eval-and-compile
  (require 'clutch-test-common)
  (require 'clutch-test-backends))

;;;; Test configuration

(defvar mysql-tls-verify-server)

(defvar clutch-column-displayers)

(defvar clutch--result-source-table)

(defvar clutch--connection-params)

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
(require 'clutch-test-connection)
(require 'clutch-test-debug)
(require 'clutch-test-live)
(require 'clutch-document)

;;;; Test backend matrix

(ert-deftest clutch-test-backend-matrix-selects-live-workflow-capabilities ()
  "Live backend matrix should replace hard-coded workflow backend lists."
  (let ((clutch-test-backend 'jdbc)
        (clutch-test-url "jdbc:duckdb:/tmp/clutch-test.duckdb"))
    (should (eq (clutch-test-live-backend-id) 'duckdb))
    (should (clutch-test-live-backend-capability-p :result-workflow))
    (should (clutch-test-live-backend-capability-p :updateable-workflow)))
  (let ((clutch-test-backend 'clickhouse)
        (clutch-test-url nil))
    (should (eq (clutch-test-live-backend-id) 'clickhouse))
    (should (clutch-test-live-backend-capability-p :result-workflow))
    (should-not
     (clutch-test-live-backend-capability-p :updateable-workflow)))
  (should (clutch-test-live-backend-capability-p :object-describe 'mysql))
  (should (clutch-test-live-backend-capability-p :ctid-row-identity 'pg))
  (should-not (clutch-test-live-backend-capability-p :result-workflow
                                                     'mongodb))
  (should (string-match-p
           "MySQL/PostgreSQL"
           (clutch-test-capability-skip-message :object-describe))))

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

(ert-deftest clutch-test-transient-state-display-highlights-active-choice ()
  "Finite transient state displays should highlight exactly one active choice."
  (let ((display (clutch--transient-state-display
                  'manual '((manual . "manual") (auto . "auto")))))
    (should (equal (substring-no-properties display) "(manual|auto)"))
    (should (eq (get-text-property (string-match "manual" display)
                                  'face display)
                'transient-value))
    (should (eq (get-text-property (string-match "auto" display)
                                  'face display)
                'transient-inactive-value))))

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

(ert-deftest clutch-test-json-view-mode-uses-json-mode-without-json-ts-grammar ()
  "JSON viewers should use `json-mode' when the tree-sitter grammar is unavailable."
  (let (selected-mode)
    (with-temp-buffer
      (insert "{\"ok\":true}")
      (cl-letf (((symbol-function 'json-ts-mode)
                 (lambda () (ert-fail "json-ts-mode should not run without a JSON grammar")))
                ((symbol-function 'treesit-language-available-p)
                 (lambda (_language) nil))
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

(ert-deftest clutch-test-value-placeholder-keeps-xml-text-and-detects-blob ()
  "Grid placeholders should keep XML readable and compactly mark BLOB values."
  (should-not (clutch--value-placeholder "{\"a\":1}" '(:type-category json)))
  (should-not (clutch--value-placeholder "{\"a\":1}" '(:type-category blob)))
  (should-not (clutch--value-placeholder "<root/>" '(:type-category text)))
  (should-not (clutch--value-placeholder "<root/>" '(:type-category blob)))
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

(ert-deftest clutch-test-view-xml-value-strips-only-generated-declaration ()
  "XML viewer should hide declarations generated by xmllint, not raw ones."
  (dolist (case '(("<root><a>1</a></root>"
                  "<?xml version=\"1.0\"?>\n<root>\n  <a>1</a>\n</root>\n"
                  nil)
                 ("<?xml version=\"1.0\"?><root><a>1</a></root>"
                  "<?xml version=\"1.0\"?>\n<root>\n  <a>1</a>\n</root>\n"
                  t)))
    (pcase-let ((`(,raw ,formatted ,expect-declaration) case))
      (let (buf)
        (cl-letf (((symbol-function 'executable-find) (lambda (_cmd) t))
                  ((symbol-function 'call-process-region)
                   (lambda (start end _program delete _destination _display &rest _args)
                     (when delete
                       (delete-region start end))
                     (insert formatted)
                     0))
                  ((symbol-function 'nxml-mode) (lambda () nil))
                  ((symbol-function 'font-lock-ensure) (lambda (&rest _args) nil))
                  ((symbol-function 'jit-lock-fontify-now) (lambda (&rest _args) nil))
                  ((symbol-function 'pop-to-buffer)
                   (lambda (b &rest _args)
                     (setq buf b)
                     b)))
          (clutch--dispatch-view raw '(:type-category text))
          (with-current-buffer buf
            (if expect-declaration
                (should (string-prefix-p "<?xml" (buffer-string)))
              (should-not (string-prefix-p "<?xml" (buffer-string))))))))))

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

(defun clutch-test--fake-pixel-width (string)
  "Return deterministic mixed-width pixels for STRING."
  (cl-loop for i below (length string)
           for display = (get-text-property i 'display string)
           sum (cond
                ((equal display "") 0)
                ((and (consp display) (eq (car display) 'space))
                 (let ((width (plist-get (cdr display) :width)))
                   (if (consp width) (car width) width)))
                ((memq (aref string i) '(?中 ?文)) 30)
                (t 10))))

(ert-deftest clutch-test-string-pad ()
  "Test string padding."
  ;; Left-align (default)
  (should (equal (clutch--string-pad "hi" 5) "hi   "))
  ;; Right-align
  (should (equal (clutch--string-pad "hi" 5 t) "   hi"))
  ;; String longer than width — no padding
  (should (equal (clutch--string-pad "hello" 3) "hello")))

(ert-deftest clutch-test-result-grid-aligns-mixed-width-font-fallbacks ()
  "Result headers and rows should share measured graphical column widths."
  (with-temp-buffer
    (clutch-result-mode)
    (setq-local clutch--result-columns '("name")
                clutch--result-column-defs '(nil)
                clutch--result-rows '(("中文"))
                clutch--filtered-rows nil
                clutch--pending-edits nil
                clutch--pending-deletes nil
                clutch--pending-inserts nil
                clutch--marked-rows nil
                clutch--sort-column nil
                clutch--sort-descending nil
                clutch--page-current 0
                clutch--page-total-rows 1
                clutch--column-widths [4])
    (cl-letf (((symbol-function 'display-graphic-p)
               (lambda (&optional _display) t))
              ((symbol-function 'default-font-width)
               (lambda () 10))
              ((symbol-function 'string-pixel-width)
               #'clutch-test--fake-pixel-width)
              ((symbol-function 'clutch--header-label)
               (lambda (name &optional _include-unsorted-sort)
                 (propertize name 'clutch-header-name t)))
              ((symbol-function 'clutch--refresh-footer-line) #'ignore))
      (clutch--render-result)
      (should (= (clutch-test--fake-pixel-width clutch--header-line-string)
                 (clutch-test--fake-pixel-width
                  (string-trim-right (buffer-string)))))
      (should (= (string-width clutch--header-line-string)
                 (string-width (string-trim-right (buffer-string)))))
      (should (equal clutch--column-widths [4]))
      (should (equal clutch--column-pixel-widths [60])))))

(ert-deftest clutch-test-result-grid-skips-pixel-layout-when-logical-widths-match ()
  "Graphical result rendering should skip pixel layout for matching cell metrics."
  (with-temp-buffer
    (clutch-result-mode)
    (setq-local clutch--result-columns '("name")
                clutch--result-column-defs '(nil)
                clutch--result-rows '(("中文"))
                clutch--filtered-rows nil
                clutch--pending-edits nil
                clutch--pending-deletes nil
                clutch--pending-inserts nil
                clutch--marked-rows nil
                clutch--sort-column nil
                clutch--sort-descending nil
                clutch--page-current 0
                clutch--page-total-rows 1
                clutch--column-widths [4])
    (cl-letf (((symbol-function 'display-graphic-p)
               (lambda (&optional _display) t))
              ((symbol-function 'default-font-width)
               (lambda () 10))
              ((symbol-function 'string-pixel-width)
               (lambda (string) (* 10 (string-width string))))
              ((symbol-function 'clutch--header-label)
               (lambda (name &optional _include-unsorted-sort)
                 name))
              ((symbol-function 'clutch--refresh-footer-line) #'ignore))
      (clutch--render-result)
      (should-not clutch--column-pixel-widths)
      (should-not clutch--column-pixel-metric)
      (should-not clutch--column-pixel-logical-widths))))

(ert-deftest clutch-test-result-grid-measures-cell-pixels-once-per-render ()
  "Graphical result rendering should not remeasure each cell during padding."
  (let ((calls 0))
    (with-temp-buffer
      (clutch-result-mode)
      (setq-local clutch--result-columns '("id" "name")
                  clutch--result-column-defs '(nil nil)
                  clutch--result-rows (cl-loop for i from 1 to 20
                                               collect (list i
                                                             (format "name-%02d"
                                                                     i)))
                  clutch--filtered-rows nil
                  clutch--pending-edits nil
                  clutch--pending-deletes nil
                  clutch--pending-inserts nil
                  clutch--marked-rows nil
                  clutch--sort-column nil
                  clutch--sort-descending nil
                  clutch--page-current 0
                  clutch--page-total-rows 20
                  clutch--column-widths [4 8])
      (cl-letf (((symbol-function 'display-graphic-p)
                 (lambda (&optional _display) t))
                ((symbol-function 'default-font-width)
                 (lambda () 10))
                ((symbol-function 'string-pixel-width)
                 (lambda (string)
                   (cl-incf calls)
                   (+ (* 10 (string-width string))
                      (if (string-search "中" string) 10 0))))
                ((symbol-function 'clutch--header-label)
                 (lambda (name &optional _include-unsorted-sort)
                   name))
                ((symbol-function 'clutch--refresh-footer-line) #'ignore))
        (clutch--render-result)
        ;; A naive graphical render measures every body cell as a full string.
        ;; Plain result cells should use the cheaper per-character cache.
        (should (< calls 40))))))

(ert-deftest clutch-test-result-grid-reuses-cell-pixel-cache-across-renders ()
  "Graphical result rendering should reuse unchanged cell pixel measurements."
  (let ((calls 0)
        (content-calls 0))
    (with-temp-buffer
      (clutch-result-mode)
      (setq-local clutch--result-columns '("id" "name")
                  clutch--result-column-defs '(nil nil)
                  clutch--result-rows (cl-loop for i from 1 to 20
                                               collect (list i
                                                             (format "name-%02d"
                                                                     i)))
                  clutch--filtered-rows nil
                  clutch--pending-edits nil
                  clutch--pending-deletes nil
                  clutch--pending-inserts nil
                  clutch--marked-rows nil
                  clutch--sort-column nil
                  clutch--sort-descending nil
                  clutch--page-current 0
                  clutch--page-total-rows 20
                  clutch--column-widths [4 8])
      (let ((content-fn (symbol-function 'clutch--cell-display-content)))
        (cl-letf (((symbol-function 'display-graphic-p)
                   (lambda (&optional _display) t))
                  ((symbol-function 'default-font-width)
                   (lambda () 10))
                  ((symbol-function 'string-pixel-width)
                   (lambda (string)
                     (cl-incf calls)
                     (+ (* 10 (string-width string))
                        (if (string-search "中" string) 10 0))))
                  ((symbol-function 'clutch--cell-display-content)
                   (lambda (&rest args)
                     (cl-incf content-calls)
                     (apply content-fn args)))
                  ((symbol-function 'clutch--header-label)
                   (lambda (name &optional _include-unsorted-sort)
                     name))
                  ((symbol-function 'clutch--refresh-footer-line) #'ignore))
          (clutch--render-result)
          (let ((first-pixel-calls calls))
            (should (= content-calls 40))
            (setq-local clutch--result-rows (reverse clutch--result-rows))
            (clutch--render-result)
            ;; Same values in a different order should not rebuild body cell
            ;; display strings.
            (should (= content-calls 40))
            (should (< (- calls first-pixel-calls) 10)))
          (aset clutch--column-widths 0 5)
          (clutch--render-result)
          ;; Changing one column width should not rescan all body columns.
          (should (= content-calls 60)))))))

(ert-deftest clutch-test-pixel-padding-uses-plain-spaces-for-cell-widths ()
  "Pixel padding should avoid display specs when spaces already match."
  (cl-letf (((symbol-function 'default-font-width) (lambda () 10)))
    (let ((padded (clutch--pad-display-string "x" 3 30 nil 10)))
      (should (equal padded "x  "))
      (should-not
       (text-property-not-all 0 (length padded) 'display nil padded)))))

(ert-deftest clutch-test-plain-string-pixel-width-falls-back-for-zero-width-chars ()
  "Plain string pixel caching should not split composed grapheme sequences."
  (cl-letf (((symbol-function 'string-pixel-width)
             (lambda (string)
               (if (string= string "é") 9 (length string)))))
    (should (= (clutch--plain-string-pixel-width "é" '(metric)) 9))))

(ert-deftest clutch-test-result-grid-header-icons-use-one-arg-pixel-width ()
  "Header icon rendering should work with Emacs 30 `string-pixel-width'."
  (with-temp-buffer
    (clutch-result-mode)
    (setq-local clutch--result-columns '("email_id")
                clutch--result-column-defs '((:name "email_id"
                                              :type-category numeric))
                clutch--result-rows '((29801412))
                clutch--filtered-rows nil
                clutch--pending-edits nil
                clutch--pending-deletes nil
                clutch--pending-inserts nil
                clutch--marked-rows nil
                clutch--sort-column nil
                clutch--sort-descending nil
                clutch--page-current 0
                clutch--page-total-rows 1
                clutch--column-widths [11])
    (cl-letf (((symbol-function 'display-graphic-p)
               (lambda (&optional _display) t))
              ((symbol-function 'default-font-width)
               (lambda () 10))
              ((symbol-function 'string-pixel-width)
               (lambda (string) (* 10 (string-width string))))
              ((symbol-function 'clutch--icon)
               (lambda (_spec &optional _fallback)
                 (propertize "S" 'face 'icon-face)))
              ((symbol-function 'clutch--refresh-footer-line) #'ignore))
      (clutch--render-result)
      (should (string-match-p "email_id"
                              (substring-no-properties
                               clutch--header-line-string))))))

(ert-deftest clutch-test-install-page-state-preserves-render-cache-for-same-shape ()
  "Page refreshes with the same columns should preserve cell render caches."
  (with-temp-buffer
    (clutch-result-mode)
    (let ((cell-cache (make-hash-table :test 'equal))
          (char-cache (make-hash-table :test 'eql))
          (columns '((:name "id") (:name "name"))))
      (setq-local clutch--result-columns '("id" "name")
                  clutch--result-column-defs columns
                  clutch--column-widths [4 8]
                  clutch--cell-render-cache cell-cache
                  clutch--cell-render-cache-signature 'cell-signature
                  clutch--char-pixel-width-cache char-cache
                  clutch--char-pixel-width-cache-signature 'char-signature)
      (clutch-result--install-page-state columns '((2 "bob")) 0.1 0)
      (should (eq clutch--cell-render-cache cell-cache))
      (should (eq clutch--char-pixel-width-cache char-cache))
      (clutch-result--install-page-state columns '((3 "eve")) 0.1 1)
      (should-not clutch--cell-render-cache)
      (should (eq clutch--char-pixel-width-cache char-cache))
      (setq-local clutch--cell-render-cache cell-cache)
      (clutch-result--install-page-state '((:name "other")) '(("x")) 0.1 0)
      (should-not clutch--cell-render-cache)
      (should-not clutch--char-pixel-width-cache))))

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

(ert-deftest clutch-test-compute-column-widths-shows-short-json ()
  "JSON columns should size to short compact values instead of placeholders."
  (let* ((clutch-column-width-max 30)
         (col-names '("j"))
         (rows '(("{\"a\":1}")))
         (columns '((:name "j" :type-category json)))
         (widths (clutch--compute-column-widths col-names rows columns)))
    (should (= (aref widths 0) (string-width "{\"a\":1}")))))

(ert-deftest clutch-test-compute-column-widths-keeps-blob-compact ()
  "BLOB columns should still use compact default width."
  (let* ((clutch-column-width-max 30)
         (col-names '("payload"))
         (rows '(("this blob text is intentionally long")))
         (columns '((:name "payload" :type-category blob)))
         (widths (clutch--compute-column-widths col-names rows columns)))
    (should (= (aref widths 0) 10))))

(ert-deftest clutch-test-compute-column-widths-shows-structured-blob-text ()
  "Structured text in BLOB columns should size like displayable text."
  (let* ((clutch-column-width-max 30)
         (col-names '("payload"))
         (rows '(("{\"a\":1,\"b\":2}")))
         (columns '((:name "payload" :type-category blob)))
         (widths (clutch--compute-column-widths col-names rows columns)))
    (should (= (aref widths 0) (string-width "{\"a\":1,\"b\":2}")))))

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
  "Visible column names should not include hidden row identity aliases."
  (with-temp-buffer
    (setq-local clutch--result-columns '("clutch__rid_0" "id" "name")
                clutch--result-column-defs
                '((:name "clutch__rid_0" :hidden t)
                  (:name "id")
                  (:name "name")))
    (should (equal (clutch--visible-column-names) '("id" "name")))))

(ert-deftest clutch-test-goto-column-skips-hidden-columns ()
  "Column completion should expose visible names and retain source indices."
  (with-temp-buffer
    (setq-local clutch--result-columns '("clutch__rid_0" "id" "name")
                clutch--result-column-defs
                '((:name "clutch__rid_0" :hidden t)
                  (:name "id")
                  (:name "name")))
    (let (candidates target-index)
      (cl-letf (((symbol-function 'completing-read)
                 (lambda (_prompt collection &rest _)
                   (setq candidates collection)
                   "name"))
                ((symbol-function 'clutch-result--goto-col-idx)
                 (lambda (index) (setq target-index index))))
        (clutch-result-goto-column))
      (should (equal candidates '("id" "name")))
      (should (= target-index 2)))))

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

(ert-deftest clutch-test-row-identity-prep-records-metadata-errors ()
  "Row identity preparation should keep metadata errors visible."
  (cl-letf (((symbol-function 'clutch-db-row-identity-candidates)
             (lambda (_conn _table)
               (signal 'clutch-db-error '("metadata failed")))))
    (let ((prep (clutch--prepare-row-identity-query
                 'fake-conn "SELECT name FROM users")))
      (should (equal (plist-get prep :identity-status) 'error))
      (should (equal (plist-get prep :identity-error-message)
                     "metadata failed"))
      (should-not (plist-get prep :candidate))
      (should (equal (plist-get prep :sql) "SELECT name FROM users")))))

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

(ert-deftest clutch-test-column-width-commands-throttle-redraws ()
  "Repeated column width commands should update widths before one redraw."
  (let ((next-timer 0)
        scheduled
        (refresh-count 0))
    (with-temp-buffer
      (insert (propertize "x" 'clutch-col-idx 0))
      (goto-char (point-min))
      (clutch-result-mode)
      (goto-char (point-min))
      (setq-local clutch--column-widths [10])
      (let ((clutch-column-width-step 5))
        (cl-letf (((symbol-function 'timerp)
                   (lambda (timer)
                     (and (consp timer) (eq (car timer) 'fake-timer))))
                  ((symbol-function 'cancel-timer)
                   #'ignore)
                  ((symbol-function 'run-at-time)
                   (lambda (delay repeat fn &rest args)
                     (let ((timer (list 'fake-timer
                                        (cl-incf next-timer))))
                       (push (list timer delay repeat fn args) scheduled)
                       timer)))
                  ((symbol-function 'clutch--refresh-display)
                   (lambda ()
                     (cl-incf refresh-count))))
          (clutch-result-widen-column)
          (should (= (aref clutch--column-widths 0) 15))
          (should (= refresh-count 0))
          (should (= (length scheduled) 1))

          (clutch-result-narrow-column)
          (should (= (aref clutch--column-widths 0) 10))
          (clutch-result-narrow-column)
          (should (= (aref clutch--column-widths 0) 5))
          (should (= refresh-count 0))
          (should (= (length scheduled) 1))

          (pcase-let ((`(,_timer ,delay ,repeat ,fn ,args)
                       (car scheduled)))
            (should (= delay clutch--column-width-refresh-delay))
            (should-not repeat)
            (should (eq fn #'clutch--run-column-width-refresh))
            (apply fn args))
          (should (= refresh-count 1))
          (should-not clutch--column-width-refresh-timer)

          (clutch-result-widen-column)
          (should (= (aref clutch--column-widths 0) 10))
          (should (= (length scheduled) 2)))))))

(ert-deftest clutch-test-column-width-commands-skip-post-command-ui-refresh ()
  "Column width commands should not do cursor-only post-command UI work."
  (let ((footer-count 0)
        (header-count 0)
        (row-count 0))
    (with-temp-buffer
      (insert (propertize "x" 'clutch-row-idx 0 'clutch-col-idx 0))
      (goto-char (point-min))
      (clutch-result-mode)
      (goto-char (point-min))
      (setq-local clutch--column-widths [10])
      (cl-letf (((symbol-function 'clutch--refresh-footer-cursor)
                 (lambda ()
                   (cl-incf footer-count)))
                ((symbol-function 'clutch--refresh-header-line)
                 (lambda ()
                   (cl-incf header-count)))
                ((symbol-function 'clutch--update-row-highlight)
                 (lambda ()
                   (cl-incf row-count))))
        (let ((this-command 'clutch-result-widen-column))
          (run-hooks 'post-command-hook))
        (should (= footer-count 0))
        (should (= header-count 0))
        (should (= row-count 0))))))

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

(ert-deftest clutch-test-refresh-display-preserves-last-cell-column ()
  "Refreshing from row chrome should restore the last resolved cell column."
  (with-temp-buffer
    (clutch-test--setup-rendered-result)
    (clutch--goto-cell 1 2)
    (let ((row-start (aref clutch--row-start-positions 1)))
      (goto-char row-start)
      (should (= (clutch--row-idx-at-line) 1))
      (should-not (get-text-property (point) 'clutch-col-idx))
      (clutch--refresh-display)
      (should (= (get-text-property (point) 'clutch-row-idx) 1))
      (should (= (get-text-property (point) 'clutch-col-idx) 2)))))

(ert-deftest clutch-test-refresh-display-measures-displayed-result-window ()
  "Refresh should render with the result window's font metrics."
  (let* ((source-win (selected-window))
         (result-win (split-window-right))
         (buf (get-buffer-create " *clutch-window-metric-test*")))
    (unwind-protect
        (progn
          (set-window-buffer result-win buf)
          (select-window source-win)
          (with-current-buffer buf
            (erase-buffer)
            (clutch-result-mode)
            (setq-local clutch--result-columns '("name")
                        clutch--result-column-defs '(nil)
                        clutch--result-rows '(("aa"))
                        clutch--filtered-rows nil
                        clutch--pending-edits nil
                        clutch--pending-deletes nil
                        clutch--pending-inserts nil
                        clutch--marked-rows nil
                        clutch--sort-column nil
                        clutch--sort-descending nil
                        clutch--page-current 0
                        clutch--page-total-rows 1
                        clutch--column-widths [4])
            (cl-letf (((symbol-function 'display-graphic-p)
                       (lambda (&optional _display) t))
                      ((symbol-function 'default-font-width)
                       (lambda ()
                         (if (eq (selected-window) result-win) 20 10)))
                      ((symbol-function 'string-pixel-width)
                       (lambda (string)
                         (+ (* (string-width string) (default-font-width))
                            (if (string-search "中" string) 10 0))))
                      ((symbol-function 'clutch--header-label)
                       (lambda (name &optional _include-unsorted-sort)
                         name))
                      ((symbol-function 'clutch--refresh-footer-line) #'ignore))
              (clutch--refresh-display)
              (should (equal clutch--column-pixel-widths [80])))))
      (when (window-live-p result-win)
        (delete-window result-win))
      (when (buffer-live-p buf)
        (kill-buffer buf)))))

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

(ert-deftest clutch-test-replace-row-at-index-refreshes-when-pixel-target-grows ()
  "Row replacement should redraw fully when graphical column pixels grow."
  (with-temp-buffer
    (clutch-result-mode)
    (setq-local clutch--result-columns '("name")
                clutch--result-column-defs '(nil)
                clutch--result-rows '(("aa"))
                clutch--filtered-rows nil
                clutch--pending-edits nil
                clutch--pending-deletes nil
                clutch--pending-inserts nil
                clutch--marked-rows nil
                clutch--sort-column nil
                clutch--sort-descending nil
                clutch--page-current 0
                clutch--page-total-rows 1
                clutch--column-widths [4])
    (cl-letf (((symbol-function 'display-graphic-p)
               (lambda (&optional _display) t))
              ((symbol-function 'default-font-width)
               (lambda () 10))
              ((symbol-function 'string-pixel-width)
               #'clutch-test--fake-pixel-width)
              ((symbol-function 'clutch--header-label)
               (lambda (name &optional _include-unsorted-sort)
                 name))
              ((symbol-function 'clutch--refresh-footer-line) #'ignore))
      (clutch--render-result)
      (should (equal clutch--column-pixel-widths [40]))
      (setq-local clutch--result-rows '(("中文")))
      (let (refreshed)
        (cl-letf (((symbol-function 'clutch--refresh-display)
                   (lambda ()
                     (setq refreshed t))))
          (clutch--replace-row-at-index 0)
          (should refreshed))))))

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
           (actual (clutch--render-row-line
                    (nth 1 clutch--result-rows) 1 (clutch--visible-columns)
                    (clutch--effective-widths) (clutch--row-number-digits)
                    render-state)))
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

(ert-deftest clutch-test-render-cell-displays-null-placeholder ()
  "Result cells should display database NULL as a compact placeholder."
  (with-temp-buffer
    (setq-local clutch--result-column-defs '((:name "name" :type-category text)))
    (let ((cell (clutch--render-cell '(nil) 0 0 [8] nil)))
      (should (string-match-p (regexp-quote "<null>") cell))
      (should (text-property-any 0 (length cell) 'face 'clutch-null-face cell))
      (should (equal (get-text-property 3 'clutch-full-value cell) nil)))))

(ert-deftest clutch-test-render-cell-highlights-active-edit-target ()
  "Cells open in an edit buffer should use the staged-edit highlight."
  (with-temp-buffer
    (setq-local clutch--result-column-defs '((:name "id" :type-category numeric)
                                             (:name "name" :type-category text)))
    (let ((cell (clutch--render-cell
                 '(1 "before") 0 1 [4 8]
                 (list :active-edit-cell (cons 0 1)))))
      (should (text-property-any 0 (length cell)
                                 'face 'clutch-modified-face cell)))))

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

(ert-deftest clutch-test-display-select-keeps-row-identity-metadata-errors ()
  "SELECT display should preserve row identity metadata errors without failing."
  (let ((result-name "*clutch-test-result*")
        (result (make-clutch-db-result
                 :columns '((:name "id" :type-category numeric))
                 :rows '((1)))))
    (clutch-test--with-result-buffer (result-name)
      (clutch-result--display-select
       'fake-conn "SELECT * FROM users" result 0
       '(:sql "SELECT * FROM users"
         :table "users"
         :identity-status error
         :identity-error-message "metadata failed")
       t nil (current-buffer))
      (with-current-buffer result-name
        (should (equal clutch--result-rows '((1))))
        (should (equal clutch--result-source-table "users"))
        (should (equal clutch--row-identity-status 'error))
        (should (equal clutch--row-identity-error-message
                       "metadata failed"))))))

(ert-deftest clutch-test-display-select-renders-in-result-window ()
  "Initial SELECT display should use the result window's font metrics."
  (let* ((source-win (selected-window))
         (result-win (split-window-right))
         (result-name "*clutch-window-display-result*")
         (result (make-clutch-db-result
                  :columns '((:name "name" :type-category text))
                  :rows '(("aa")))))
    (unwind-protect
        (cl-letf (((symbol-function 'clutch-result--buffer-name)
                   (lambda () result-name))
                  ((symbol-function 'clutch-result--show-buffer)
                   (lambda (buf)
                     (set-window-buffer result-win buf)
                     (select-window result-win)))
                  ((symbol-function 'clutch--load-fk-info) #'ignore)
                  ((symbol-function 'display-graphic-p)
                   (lambda (&optional _display) t))
                  ((symbol-function 'default-font-width)
                   (lambda ()
                     (if (eq (selected-window) result-win) 20 10)))
                  ((symbol-function 'string-pixel-width)
                   (lambda (string)
                     (+ (* (string-width string) (default-font-width))
                        (if (string-search "中" string) 10 0))))
                  ((symbol-function 'clutch--header-label)
                   (lambda (name &optional _include-unsorted-sort)
                     name))
                  ((symbol-function 'clutch--refresh-footer-line) #'ignore))
          (with-temp-buffer
            (select-window source-win)
            (clutch-result--display-select
             'fake-conn "SELECT name FROM users" result 0 nil t nil
             (current-buffer)))
          (with-current-buffer result-name
            (should (equal clutch--column-pixel-widths [100]))))
      (when (window-live-p result-win)
        (delete-window result-win))
      (when-let* ((buf (get-buffer result-name)))
        (kill-buffer buf)))))

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

(ert-deftest clutch-test-install-page-state-keeps-manual-widths-for-same-columns ()
  "Page refresh should preserve adjusted widths when columns are unchanged."
  (with-temp-buffer
    (setq-local clutch--result-columns '("id" "name")
                clutch--column-widths [12 7]
                clutch-result-max-rows 50)
    (clutch-result--install-page-state
     '((:name "id" :type-category numeric)
       (:name "name" :type-category text))
     '((100 "a much longer customer name")) 0.1 0)
    (should (equal clutch--result-columns '("id" "name")))
    (should (equal clutch--column-widths [12 7]))))

(ert-deftest clutch-test-install-page-state-recomputes-widths-for-changed-columns ()
  "Page refresh should not reuse adjusted widths for a different column set."
  (with-temp-buffer
    (setq-local clutch--result-columns '("id" "name")
                clutch--column-widths [12 7]
                clutch-result-max-rows 50)
    (clutch-result--install-page-state
     '((:name "id" :type-category numeric)
       (:name "email" :type-category text))
     '((100 "alice@example.test")) 0.1 0)
    (should (equal clutch--result-columns '("id" "email")))
    (should-not (equal clutch--column-widths [12 7]))))

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
                                (clutch--visible-columns)
                                (clutch--effective-widths)
                                (clutch--row-number-digits)
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
              (should-not (string-match-p "DISCONNECTED" line)))))
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

(ert-deftest clutch-test-record-transient-description-follows-point-action ()
  "Record transient should name the action that RET performs at point."
  (let ((result-buf (generate-new-buffer " *clutch-record-action*")))
    (unwind-protect
        (progn
          (with-current-buffer result-buf
            (setq-local clutch--result-column-defs
                        '((:name "payload" :type-category json))
                        clutch--fk-info nil))
          (with-temp-buffer
            (insert (propertize "payload"
                                'clutch-col-idx 0
                                'clutch-row-idx 0
                                'clutch-full-value (make-string 100 ?x)))
            (goto-char (point-min))
            (setq-local clutch-record--result-buffer result-buf
                        clutch-record--expanded-fields nil)
            (should (equal (clutch-record--field-action-description) "Expand"))
            (setq-local clutch-record--expanded-fields '(0))
            (should (equal (clutch-record--field-action-description) "Collapse"))
            (setq-local clutch-record--expanded-fields nil)
            (put-text-property (point-min) (point-max)
                               'clutch-full-value "{\"id\":1}")
            (should (equal (clutch-record--field-action-description) "Show value"))
            (with-current-buffer result-buf
              (setq-local clutch--fk-info '((0 . (:ref-table "users")))))
            (should (equal (clutch-record--field-action-description) "Follow FK"))
            (with-current-buffer result-buf
              (setq-local clutch--fk-info nil
                          clutch--result-column-defs
                          '((:name "payload" :type-category text))))
            (should (equal (clutch-record--field-action-description) "Show value"))
            (goto-char (point-max))
            (should (equal (clutch-record--field-action-description)
                           "Field action unavailable"))))
      (kill-buffer result-buf))))

(ert-deftest clutch-test-record-toggle-expand-uses-shared-action-context ()
  "Record RET should execute expand, collapse, and foreign-key actions."
  (let ((result-buf (generate-new-buffer " *clutch-record-command*")))
    (unwind-protect
        (progn
          (with-current-buffer result-buf
            (setq-local clutch--result-column-defs
                        '((:name "payload" :type-category json))
                        clutch--fk-info nil))
          (with-temp-buffer
            (insert (propertize "payload"
                                'clutch-col-idx 0
                                'clutch-row-idx 0
                                'clutch-full-value (make-string 100 ?x)))
            (goto-char (point-min))
            (setq-local clutch-record--result-buffer result-buf
                        clutch-record--expanded-fields nil)
            (let (followed
                  (render-count 0))
              (cl-letf (((symbol-function 'clutch-record--render)
                         (lambda () (cl-incf render-count)))
                        ((symbol-function 'clutch-record--follow-fk)
                         (lambda (fk value source)
                           (setq followed (list fk value source)))))
                (clutch-record-toggle-expand)
                (should (equal clutch-record--expanded-fields '(0)))
                (clutch-record-toggle-expand)
                (should-not clutch-record--expanded-fields)
                (should (= render-count 2))
                (with-current-buffer result-buf
                  (setq-local clutch--fk-info
                              '((0 . (:ref-table "users"
                                      :ref-column "id")))))
                (clutch-record-toggle-expand)
                (should (equal followed
                               (list '(:ref-table "users" :ref-column "id")
                                     (make-string 100 ?x) result-buf)))))))
      (kill-buffer result-buf))))

(ert-deftest clutch-test-render-separator ()
  "Test table separator line rendering."
  (let ((visible-cols '(0 1 2))
        (widths [5 10 8]))
    (let ((sep (clutch--render-separator visible-cols widths 'top)))
      (should (stringp sep))
      (should (> (length sep) 0)))))

;;;; Rendering — header-line and footer

(ert-deftest clutch-test-header-key-hint-uses-distinct-faces ()
  "Header shortcut hints should style keys separately from descriptions."
  (let ((hint (clutch--key-hint "C-c C-c" "stage")))
    (should (equal (substring-no-properties hint) "C-c C-c: stage"))
    (should (eq (get-text-property 0 'face hint)
                'clutch-key-hint-key-face))
    (should (eq (get-text-property (length "C-c C-c: ") 'face hint)
                'clutch-key-hint-description-face))))

(ert-deftest clutch-test-header-line-with-hscroll-matches-body-offset ()
  "Header-line hscroll should track body hscroll exactly."
  (with-temp-buffer
    (setq-local clutch--header-line-string "0123456789")
    (cl-letf (((symbol-function 'window-hscroll) (lambda (&optional _window) 3)))
      (should (equal (clutch--header-line-with-hscroll) "3456789")))))

(ert-deftest clutch-test-header-line-with-hscroll-crops-display-space-by-pixels ()
  "Header-line hscroll should preserve display-space remainder."
  (with-temp-buffer
    (let ((header (copy-sequence " x")))
      (put-text-property 0 1 'display '(space :width (30)) header)
      (setq-local clutch--header-line-string header
                  clutch--column-pixel-widths [30])
      (cl-letf (((symbol-function 'display-graphic-p)
                 (lambda (&optional _display) t))
                ((symbol-function 'default-font-width)
                 (lambda () 10))
                ((symbol-function 'window-hscroll)
                 (lambda (&optional _window) 1))
                ((symbol-function 'string-pixel-width)
                 #'clutch-test--fake-pixel-width))
        (let ((cropped (clutch--header-line-with-hscroll)))
          (should (equal (substring-no-properties cropped) " x"))
          (should (equal (get-text-property 0 'display cropped)
                         '(space :width (20))))
          (should (= (clutch-test--fake-pixel-width cropped) 30)))))))

(ert-deftest clutch-test-header-line-display-prefixes-align-to-zero ()
  "Header-line display should remain aligned to the window's left edge."
  (with-temp-buffer
    (setq-local clutch--header-line-string "abc")
    (cl-letf (((symbol-function 'window-hscroll) (lambda (&optional _window) 0)))
      (let ((rendered (clutch--header-line-display)))
        (should (equal (substring rendered 1) "abc"))
        (should (equal (get-text-property 0 'display rendered)
                       '(space :align-to 0)))))))

(ert-deftest clutch-test-header-line-display-refreshes-stale-pixel-metrics ()
  "Header-line redisplay should schedule refresh after font metrics change."
  (with-temp-buffer
    (clutch-result-mode)
    (setq-local clutch--result-columns '("name")
                clutch--result-column-defs '(nil)
                clutch--result-rows '(("中文"))
                clutch--filtered-rows nil
                clutch--pending-edits nil
                clutch--pending-deletes nil
                clutch--pending-inserts nil
                clutch--marked-rows nil
                clutch--sort-column nil
                clutch--sort-descending nil
                clutch--page-current 0
                clutch--page-total-rows 1
                clutch--column-widths [4])
    (cl-letf (((symbol-function 'display-graphic-p)
               (lambda (&optional _display) t))
              ((symbol-function 'default-font-width)
               (lambda () 10))
              ((symbol-function 'string-pixel-width)
               #'clutch-test--fake-pixel-width)
              ((symbol-function 'clutch--header-label)
               (lambda (name &optional _include-unsorted-sort)
                 name))
              ((symbol-function 'clutch--refresh-footer-line) #'ignore))
      (clutch--render-result)
      (setq-local face-remapping-alist '((default (:height 2.0))))
      (let (scheduled)
        (cl-letf (((symbol-function 'run-at-time)
                   (lambda (&rest args)
                     (setq scheduled args)
                     (timer-create)))
                  ((symbol-function 'window-hscroll)
                   (lambda (&optional _window) 0)))
          (clutch--header-line-display)
          (should scheduled)
          (should (eq (nth 2 scheduled) #'clutch--run-column-width-refresh))
          (should (eq (nth 3 scheduled) (current-buffer))))))))

(ert-deftest clutch-test-header-line-display-refreshes-when-pixel-layout-becomes-unneeded ()
  "Header-line redisplay should refresh stale pixel layout when metrics match."
  (with-temp-buffer
    (clutch-result-mode)
    (setq-local clutch--header-line-string "name"
                clutch--column-pixel-widths [40]
                clutch--column-pixel-metric '(old))
    (let (scheduled)
      (cl-letf (((symbol-function 'display-graphic-p)
                 (lambda (&optional _display) t))
                ((symbol-function 'default-font-width)
                 (lambda () 10))
                ((symbol-function 'string-pixel-width)
                 (lambda (string) (* 10 (string-width string))))
                ((symbol-function 'window-hscroll)
                 (lambda (&optional _window) 0))
                ((symbol-function 'run-at-time)
                 (lambda (&rest args)
                   (setq scheduled args)
                   (timer-create))))
        (clutch--header-line-display)
        (should scheduled)
        (should (eq (nth 2 scheduled) #'clutch--run-column-width-refresh))
        (should (eq (nth 3 scheduled) (current-buffer)))))))

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
    (cl-letf (((symbol-function 'clutch--fixed-width-icon)
               (lambda (spec _fallback &optional _face)
                 (pcase (cdr spec)
                   ("nf-oct-sort_desc" "D")
                   (_ "N")))))
      (clutch-test--setup-rendered-result)
      (let ((body (buffer-string))
            (before (substring-no-properties clutch--header-line-string)))
        (setq-local clutch--sort-column "name"
                    clutch--sort-descending t)
        (clutch--refresh-header-line)
        (should (equal body (buffer-string)))
        (should-not (equal before
                           (substring-no-properties clutch--header-line-string)))))))

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

(ert-deftest clutch-test-fixed-width-icon-pads-partial-cell-graphic-glyph ()
  "Graphical icons should reserve enough full text cells for their pixels."
  (cl-letf (((symbol-function 'clutch--icon)
             (lambda (_spec &optional _fallback)
               (propertize "I" 'face 'icon-face)))
            ((symbol-function 'display-graphic-p)
             (lambda (&optional _display) t))
            ((symbol-function 'default-font-width)
             (lambda () 10))
            ((symbol-function 'string-pixel-width)
             (lambda (_string) 17)))
    (let* ((icon (clutch--fixed-width-icon '(mdicon . "nf-md-sort")
                                           nil))
           (glyph (get-text-property 0 'display icon))
           (padding (get-text-property 1 'display icon)))
      (should (= (length icon) 2))
      (should (= (string-width icon) 2))
      (should (stringp glyph))
      (should (equal (substring-no-properties glyph) "I"))
      (should (equal padding '(space :width (3)))))))

(ert-deftest clutch-test-fixed-width-icon-keeps-single-cell-graphic-glyph ()
  "Icons that already fill their logical cell should not gain padding."
  (cl-letf (((symbol-function 'clutch--icon)
             (lambda (_spec &optional _fallback)
               (propertize "I" 'face 'icon-face)))
            ((symbol-function 'display-graphic-p)
             (lambda (&optional _display) t))
            ((symbol-function 'default-font-width)
             (lambda () 10))
            ((symbol-function 'string-pixel-width)
             (lambda (_string) 10)))
    (let ((icon (clutch--fixed-width-icon '(mdicon . "nf-md-sort")
                                          nil)))
      (should (= (string-width icon) 1))
      (should (equal (substring-no-properties icon) "I"))
      (should-not (get-text-property 0 'display icon)))))

(ert-deftest clutch-test-header-label-uses-sort-icons-without-fallbacks ()
  "Header labels should use original-size Nerd Font sort icons without fallbacks."
  (with-temp-buffer
    (let (specs fallbacks faces)
      (cl-letf (((symbol-function 'clutch--fixed-width-icon)
                 (lambda (spec fallback &optional face)
                   (push spec specs)
                   (push fallback fallbacks)
                   (push face faces)
                   "S")))
        (should (equal (substring-no-properties (clutch--header-label "id"))
                       "id"))
        (should (equal (substring-no-properties
                        (clutch--header-label "id" t))
                       "id S"))
        (should (equal (car specs) '(mdicon . "nf-md-sort")))
        (should-not (car fallbacks))
        (should-not (car faces))
        (setq-local clutch--sort-column "id"
                    clutch--sort-descending nil
                    specs nil
                    fallbacks nil
                    faces nil)
        (should (equal (substring-no-properties (clutch--header-label "id"))
                       "id S"))
        (should (equal (car specs) '(octicon . "nf-oct-sort_asc")))
        (should-not (car fallbacks))
        (should-not (car faces))
        (setq-local clutch--sort-descending t
                    specs nil
                    fallbacks nil
                    faces nil)
        (should (equal (substring-no-properties (clutch--header-label "id"))
                       "id S"))
        (should (equal (car specs) '(octicon . "nf-oct-sort_desc")))
        (should-not (car fallbacks))
        (should-not (car faces))))))

(ert-deftest clutch-test-header-label-omits-sort-marker-without-nerd-icons ()
  "Header labels should not substitute text when sort icons are unavailable."
  (with-temp-buffer
    (setq-local clutch--sort-column "id"
                clutch--sort-descending t)
    (cl-letf (((symbol-function 'clutch--icon)
               (lambda (_spec fallback &optional _face)
                 (should-not fallback)
                 nil)))
      (should (equal (clutch--header-label "id" t) "id")))))

(ert-deftest clutch-test-header-cell-installs-sort-click-map ()
  "Result header cells should expose a mouse keymap for sort cycling."
  (with-temp-buffer
    (setq-local clutch--result-columns '("id"))
    (cl-letf (((symbol-function 'clutch--fixed-width-icon)
               (lambda (_spec fallback &optional _face) fallback)))
      (let* ((cell (clutch--header-cell 0 (vector 6)))
             (pos (text-property-any 0 (length cell)
                                     'clutch-header-col 0 cell))
             (local-map (get-text-property pos 'local-map cell))
             (keymap (get-text-property pos 'keymap cell)))
        (should pos)
        (should (keymapp local-map))
        (should (eq keymap local-map))
        (should (lookup-key local-map [header-line mouse-1]))
        (should (lookup-key local-map [mouse-1]))
        (should (eq (get-text-property pos 'mouse-face cell)
                    'mode-line-highlight))
        (should (string-match-p "cycle sort"
                                (get-text-property pos 'help-echo cell)))))))

(ert-deftest clutch-test-header-sort-keymap-dispatches-in-event-window-buffer ()
  "The installed header command should sort in the clicked result buffer."
  (let ((source (generate-new-buffer " *clutch-source*"))
        (result (generate-new-buffer " *clutch-result*"))
        (win (selected-window))
        command
        called-buffer
        called-args)
    (unwind-protect
        (progn
          (with-current-buffer result
            (setq-local clutch--result-columns '("id" "name"))
            (setq-local clutch--header-sort-function
                        (lambda (cidx expected-name)
                          (setq called-buffer (current-buffer)
                                called-args (list cidx expected-name))))
            (cl-letf (((symbol-function 'clutch--fixed-width-icon)
                       (lambda (_spec fallback &optional _face) fallback)))
              (let* ((cell (clutch--header-cell 1 (vector 6 8)))
                     (pos (text-property-any 0 (length cell)
                                             'clutch-header-col 1 cell))
                     (map (get-text-property pos 'local-map cell)))
                (setq command
                      (lookup-key map [header-line mouse-1])))))
          (with-current-buffer source
            (cl-letf (((symbol-function 'event-start)
                       (lambda (_event) (list win)))
                      ((symbol-function 'window-buffer)
                       (lambda (_win) result)))
              (funcall-interactively command 'fake-event))))
      (kill-buffer source)
      (kill-buffer result))
    (should (eq called-buffer result))
    (should (equal called-args '(1 "name")))))

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

(ert-deftest clutch-test-render-footer-warns-when-row-identity-metadata-fails ()
  "Footer should show row identity metadata errors separately from missing identity."
  (with-temp-buffer
    (setq-local clutch--result-columns '("id" "name")
                clutch--result-source-table "users"
                clutch--row-identity-status 'error
                clutch--row-identity-error-message "metadata failed"
                clutch--last-query "SELECT * FROM users")
    (let* ((footer-prop (clutch--render-footer 10 0 500 100))
           (footer (substring-no-properties footer-prop))
           (identity-start (string-match
                            "row identity error: metadata failed" footer)))
      (should (string-match-p "row identity error: metadata failed" footer))
      (should-not (string-match-p "row identity missing" footer))
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
        (clutch--column-displayer-version 0)
        (first (lambda (_value) "first"))
        (second (lambda (_value) "second")))
    (clutch-register-column-displayer "Orders" "Status" first)
    (should (= clutch--column-displayer-version 1))
    (should (eq (clutch--lookup-column-displayer "orders" "status") first))
    (clutch-register-column-displayer "orders" "status" second)
    (should (= clutch--column-displayer-version 2))
    (should (= (length clutch-column-displayers) 1))
    (should (= (length (cdar clutch-column-displayers)) 1))
    (should (eq (clutch--lookup-column-displayer "ORDERS" "STATUS") second))
    (clutch-unregister-column-displayer "ORDERS" "Missing")
    (should (= clutch--column-displayer-version 2))
    (clutch-unregister-column-displayer "ORDERS" "STATUS")
    (should (= clutch--column-displayer-version 3))
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

(ert-deftest clutch-test-cell-render-cache-separates-source-table-displayers ()
  "Cell render caching should not reuse custom displays across source tables."
  (let ((clutch-column-displayers nil)
        (clutch--column-displayer-version 0))
    (clutch-register-column-displayer
     "users" "status"
     (lambda (value) (format "user:%s" value)))
    (clutch-register-column-displayer
     "orders" "status"
     (lambda (value) (format "order:%s" value)))
    (with-temp-buffer
      (setq-local clutch--result-columns '("status")
                  clutch--result-column-defs '((:name "status"))
                  clutch--result-source-table "users")
      (cl-letf (((symbol-function 'string-pixel-width)
                 (lambda (string) (string-width string))))
        (should
         (equal (car (clutch--cached-cell-render
                      "open" 12 0 '(:name "status") nil nil '(metric)))
                "user:open"))
        (setq-local clutch--result-source-table "orders")
        (should
         (equal (car (clutch--cached-cell-render
                      "open" 12 0 '(:name "status") nil nil '(metric)))
                "order:open"))))))

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

(ert-deftest clutch-test-cell-display-content-truncates-text-with-ellipsis ()
  "Default cell text should use ellipsis when truncated."
  (with-temp-buffer
    (should (equal
             (clutch--cell-display-content
              "abcdef" 4 '(:name "status" :type-category text) nil)
             "abc…"))))

(ert-deftest clutch-test-cell-display-content-truncates-custom-displayer-output ()
  "Custom column displayer output should be truncated with ellipsis."
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
               "abc…")))))

(ert-deftest clutch-test-cell-display-content-highlights-short-json ()
  "Short JSON cells should render full compact JSON with token faces."
  (let ((s (clutch--cell-display-content
            "{\"a\":1,\"b\":\"x\"}" 20 '(:name "payload" :type-category json) nil)))
    (should (equal (substring-no-properties s) "{\"a\":1,\"b\":\"x\"}"))
    (should (eq (get-text-property 0 'face s) 'shadow))
    (should (eq (get-text-property 1 'face s) (clutch--json-key-face)))
    (should-not (eq (get-text-property 1 'face s) 'clutch-field-name-face))
    (should (eq (get-text-property 5 'face s) 'font-lock-constant-face))
    (should (eq (get-text-property 12 'face s) 'font-lock-string-face))))

(ert-deftest clutch-test-cell-display-content-truncates-long-json-unhighlighted ()
  "Long JSON cells should show a raw prefix with ellipsis, not a placeholder."
  (let ((s (clutch--cell-display-content
            "{\"status\":\"paid\",\"total\":128.5}"
            18 '(:name "payload" :type-category json) nil)))
    (should (string-suffix-p "…" s))
    (should-not (string-match-p "<JSON>" s))
    (should-not (get-text-property 0 'face s))))

(ert-deftest clutch-test-cell-display-content-truncates-json-blob-as-text ()
  "JSON text in BLOB cells should show a JSON prefix, not a BLOB placeholder."
  (let ((s (clutch--cell-display-content
            "{\"status\":\"paid\",\"total\":128.5}"
            18 '(:name "payload" :type-category blob) nil)))
    (should (string-prefix-p "{\"status\"" s))
    (should (string-suffix-p "…" s))
    (should-not (string-match-p "<BLOB>" s))
    (should-not (get-text-property 0 'face s))))

(ert-deftest clutch-test-cell-display-content-highlights-short-xml ()
  "Short XML cells should render full XML with lightweight token faces."
  (let ((s (clutch--cell-display-content
            "<root attr=\"x\"><a>1</a></root>"
            40 '(:name "payload" :type-category text) nil)))
    (should (equal (substring-no-properties s)
                   "<root attr=\"x\"><a>1</a></root>"))
    (should (eq (get-text-property 0 'face s) 'shadow))
    (should (eq (get-text-property 1 'face s) 'font-lock-function-name-face))
    (should (eq (get-text-property 6 'face s) (clutch--json-key-face)))
    (should (eq (get-text-property 11 'face s) 'font-lock-string-face))
    (should (eq (get-text-property 20 'face s) 'shadow))))

(ert-deftest clutch-test-cell-display-content-truncates-long-xml-with-ellipsis ()
  "Long XML cells should show a raw prefix with ellipsis, not a placeholder."
  (let ((s (clutch--cell-display-content
            "<order><item sku=\"A1\"/><item sku=\"B2\"/></order>"
            18 '(:name "payload" :type-category text) nil)))
    (should (string-prefix-p "<order>" s))
    (should (string-suffix-p "…" s))
    (should-not (string-match-p "<XML>" s))
    (should-not (get-text-property 0 'face s))))

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
               (lambda (_conn callback &optional _errback _idle-delay)
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
               (lambda (_conn callback &optional _errback _idle-delay)
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
                   (lambda (_conn callback &optional _errback _idle-delay)
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
  (let ((clutch-schema-refresh-idle-delay-seconds 0.75)
        sync-called
        async-called
        seen-delay)
    (cl-letf (((symbol-function 'clutch-db-eager-schema-refresh-p)
               (lambda (_conn) nil))
              ((symbol-function 'clutch--refresh-schema-cache-async)
               (lambda (_conn &optional idle-delay)
                 (setq seen-delay idle-delay)
                 (setq async-called t)
                 nil))
              ((symbol-function 'clutch--refresh-schema-cache)
               (lambda (_conn)
                 (setq sync-called t)
                 t)))
      (clutch--prime-schema-cache 'fake-conn)
      (should async-called)
      (should (= seen-delay 0.75))
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
                  clutch--connection-params '(:backend mysql)
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

(defmacro clutch-test--with-edit-cell-result-buffer (spec &rest body)
  "Bind a result buffer according to SPEC while running BODY.
SPEC has the form (VAR COLUMNS COLUMN-DEFS ROWS . LOCALS), matching
`clutch-test--make-edit-cell-result-buffer'."
  (declare (indent 1))
  (let ((var (nth 0 spec))
        (columns (nth 1 spec))
        (column-defs (nth 2 spec))
        (rows (nth 3 spec))
        (locals (nthcdr 4 spec)))
    `(let ((,var (clutch-test--make-edit-cell-result-buffer
                  ,columns ,column-defs ,rows ,@locals)))
       (unwind-protect
           (progn ,@body)
         (kill-buffer ,var)))))

(defmacro clutch-test--with-open-edit-cell (edit-var result-spec cell table details
                                                     &rest body)
  "Open EDIT-VAR from RESULT-SPEC for CELL on TABLE with DETAILS, then run BODY."
  (declare (indent 5))
  (let ((result-var (car result-spec)))
    `(clutch-test--with-edit-cell-result-buffer ,result-spec
       (let ((,edit-var (clutch-test--open-edit-cell
                         ,result-var ,cell ,table ,details)))
         ,@body))))

(defmacro clutch-test--with-result-edit-buffer (var initial-text &rest body)
  "Bind VAR to an edit buffer seeded with INITIAL-TEXT while running BODY."
  (declare (indent 2))
  `(let ((,var (generate-new-buffer "*clutch-edit-test*")))
     (unwind-protect
         (with-current-buffer ,var
           (insert ,initial-text)
           (clutch--result-edit-mode 1)
           ,@body)
       (kill-buffer ,var))))

(defmacro clutch-test--with-insert-result-buffer (spec &rest body)
  "Bind an insert result buffer according to SPEC while running BODY.
SPEC has the form (VAR COLUMNS COLUMN-DEFS . LOCALS)."
  (declare (indent 1))
  (let ((var (nth 0 spec))
        (columns (nth 1 spec))
        (column-defs (nth 2 spec))
        (locals (nthcdr 3 spec)))
    `(let ((,var (generate-new-buffer "*clutch-insert-result*")))
       (unwind-protect
           (progn
             (with-current-buffer ,var
               (setq-local clutch-connection 'fake-conn
                           clutch--result-columns ,columns
                           clutch--result-column-defs ,column-defs)
               (let ((locals (list ,@locals)))
                 (while locals
                   (set (make-local-variable (pop locals))
                        (pop locals)))))
             ,@body)
         (kill-buffer ,var)))))

(ert-deftest clutch-test-edit-pending-insert-reopens-prefilled-insert-buffer ()
  "Editing a ghost insert row should reopen the staged insert with its values."
  (let ((result-buf (generate-new-buffer "*clutch-result*")))
    (unwind-protect
          (with-current-buffer result-buf
            (setq-local clutch--result-columns '("id" "severity" "owner")
                        clutch--connection-params '(:backend mysql)
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
  (clutch-test--with-open-edit-cell buf
      (result-buf
       '("severity")
       '((:name "severity" :type-category text))
       '(("low"))
       'clutch--row-identity
       (clutch-test--primary-row-identity "shipping_incidents" '("severity") '(0)))
      '(0 0 "low")
      "shipping_incidents"
      (list (list :name "severity" :type "enum('low','medium','high')"))
    (with-current-buffer buf
      (should (string-match-p "\\[enum\\]" (format "%s" header-line-format)))
      (should (string-match-p "M-TAB: complete" (format "%s" header-line-format)))
      (should (string-match-p "C-c C-n: set NULL" (format "%s" header-line-format)))
      (should-not (string-match-p "Editing row" (format "%s" header-line-format)))
      (pcase-let ((`(,beg ,end ,candidates . ,_)
                   (clutch-result-edit-completion-at-point)))
        (should (= beg (point-min)))
        (should (= end (point-max)))
        (should (equal candidates '("low" "medium" "high")))))))

(ert-deftest clutch-test-edit-cell-opens-null-state-placeholder ()
  "Editing a NULL cell should show a placeholder while keeping buffer text empty."
  (clutch-test--with-open-edit-cell buf
      (result-buf
       '("id" "note")
       '((:name "id" :type-category numeric)
         (:name "note" :type-category text))
       '((1 nil))
       'clutch--row-identity
       (clutch-test--primary-row-identity "shipping_incidents" '("id") '(0)))
      '(0 1 nil)
      "shipping_incidents"
      (list (list :name "note" :type "text"))
    (with-current-buffer buf
      (should clutch-result-edit--null-p)
      (should (equal (buffer-string) ""))
      (should (overlayp clutch-result-edit--null-placeholder-overlay))
      (should (equal
               (substring-no-properties
                (overlay-get clutch-result-edit--null-placeholder-overlay
                             'after-string))
               "<null>"))
      (insert "hello")
      (should-not clutch-result-edit--null-p)
      (should-not (overlayp clutch-result-edit--null-placeholder-overlay))
      (should (equal (buffer-string) "hello")))))

(ert-deftest clutch-test-edit-cell-errors-clearly-without-row-identity ()
  "Edit entry should fail early when the result is not updateable."
  (clutch-test--with-edit-cell-result-buffer
      (result-buf
       '("id" "name")
       '((:name "id" :type-category numeric)
         (:name "name" :type-category text))
       '((1 "alice"))
       'clutch--result-source-table "users"
       'clutch--last-query "SELECT * FROM users")
    (cl-letf (((symbol-function 'clutch--cell-at-point)
               (lambda () '(0 1 "alice"))))
      (with-current-buffer result-buf
        (let ((err (should-error (clutch-result-edit-cell) :type 'user-error)))
          (should (string-match-p
                   "Cannot edit cell: no primary, unique, or row locator identity available for table users"
                   (error-message-string err))))
        (should-not (get-buffer "*clutch-edit: [0].name*"))))))

(ert-deftest clutch-test-edit-cell-errors-with-row-identity-metadata-error ()
  "Edit entry should report row identity metadata errors."
  (clutch-test--with-edit-cell-result-buffer
      (result-buf
       '("id" "name")
       '((:name "id" :type-category numeric)
         (:name "name" :type-category text))
       '((1 "alice"))
       'clutch--result-source-table "users"
       'clutch--row-identity-status 'error
       'clutch--row-identity-error-message "metadata failed"
       'clutch--last-query "SELECT * FROM users")
    (cl-letf (((symbol-function 'clutch--cell-at-point)
               (lambda () '(0 1 "alice"))))
      (with-current-buffer result-buf
        (let ((err (should-error (clutch-result-edit-cell) :type 'user-error)))
          (should (string-match-p
                   "Cannot edit cell: row identity metadata failed for table users: metadata failed"
                   (error-message-string err))))
        (should-not (get-buffer "*clutch-edit: [0].name*"))))))

(ert-deftest clutch-test-edit-cell-shows-temporal-now-hint ()
  "Temporal edit buffers should advertise the shared now shortcut."
  (clutch-test--with-open-edit-cell buf
      (result-buf
       '("opened_at")
       '((:name "opened_at" :type-category datetime))
       '(("2026-03-10 10:00:00"))
       'clutch--row-identity
       (clutch-test--primary-row-identity "shipping_incidents" '("opened_at") '(0)))
      '(0 0 "2026-03-10 10:00:00")
      "shipping_incidents"
      (list (list :name "opened_at" :type "datetime"))
    (with-current-buffer buf
      (should (string-match-p "\\[datetime\\]" (format "%s" header-line-format)))
      (should (string-match-p (regexp-quote "C-c .: now")
                              (format "%s" header-line-format))))))

(ert-deftest clutch-test-edit-cell-opens-json-sub-editor-directly ()
  "JSON cells should jump straight into the JSON sub-editor."
  (clutch-test--with-open-edit-cell buf
      (result-buf
       '("payload")
       '((:name "payload" :type-category json))
       '(("{\"a\":1}"))
       'clutch--row-identity
       (clutch-test--primary-row-identity "shipping_incidents" '("payload") '(0)))
      '(0 0 "{\"a\":1}")
      "shipping_incidents"
      (list (list :name "payload" :type "json"))
    (should (string-match-p "\\*clutch-edit-json: payload\\*" (buffer-name buf)))
    (with-current-buffer buf
      (should (equal clutch-result-edit-json--field-name "payload"))
      (should (string-match-p "JSON field payload" (format "%s" header-line-format)))
      (should (equal (buffer-substring-no-properties (point-min) (point-max))
                     "{\n  \"a\": 1\n}")))))

(ert-deftest clutch-test-edit-cell-json-object-opens-sub-editor-with-json-text ()
  "Parsed JSON objects should reach the JSON sub-editor as JSON text."
  (skip-unless (fboundp 'json-serialize))
  (let ((payload (make-hash-table :test 'equal)))
    (puthash "test" t payload)
    (puthash "data" (vector 1 2) payload)
    (clutch-test--with-open-edit-cell buf
        (result-buf
         '("payload")
         '((:name "payload" :type-category json))
         (list (list payload))
         'clutch--row-identity
         (clutch-test--primary-row-identity "shipping_incidents" '("payload") '(0)))
        (list 0 0 payload)
        "shipping_incidents"
        (list (list :name "payload" :type "json"))
      (with-current-buffer buf
        (let ((text (buffer-substring-no-properties (point-min) (point-max))))
          (should-not (string-match-p "#s(hash-table" text))
          (should (string-match-p "\"test\": true" text))
          (should (string-match-p "\"data\": \\[" text)))))))

(ert-deftest clutch-test-edit-cell-json-string-opens-sub-editor-with-json-text ()
  "Parsed JSON string scalars should stay valid JSON in the sub-editor."
  (skip-unless (fboundp 'json-serialize))
  (let ((payload "hello"))
    (clutch-test--with-open-edit-cell buf
        (result-buf
         '("payload")
         '((:name "payload" :type-category json))
         (list (list payload))
         'clutch--row-identity
         (clutch-test--primary-row-identity "shipping_incidents" '("payload") '(0)))
        (list 0 0 payload)
        "shipping_incidents"
        (list (list :name "payload" :type "json"))
      (with-current-buffer buf
        (should (equal (buffer-substring-no-properties (point-min) (point-max))
                       "\"hello\""))))))

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
                        clutch--connection-params '(:backend mysql)
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
                        clutch--connection-params '(:backend mysql)
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
                        clutch--connection-params '(:backend mysql)
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
    (setq-local clutch-connection 'fake-conn
                clutch--connection-params '(:backend mysql)
                clutch--last-query "SELECT * FROM users"
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
    (setq-local clutch-connection 'fake-conn
                clutch--connection-params '(:backend mysql)
                clutch--last-query "SELECT * FROM users"
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
    (setq-local clutch--connection-params '(:backend mysql))
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
  (let (staged-value quit-called err)
    (clutch-test--with-result-edit-buffer edit-buf "xx"
      (setq-local clutch-result-edit--column-name "impact_score"
                  clutch-result-edit--column-def '(:name "impact_score" :type-category numeric)
                  clutch-result-edit--column-detail '(:name "impact_score" :type "decimal(5,1)")
                  clutch-result--edit-callback (lambda (value) (setq staged-value value)))
      (cl-letf (((symbol-function 'quit-window)
                 (lambda (&rest _args) (setq quit-called t))))
        (setq err (should-error (clutch-result-edit-finish) :type 'user-error))
        (should (string-match-p "Field impact_score expects a numeric value"
                                (error-message-string err)))
        (should-not quit-called)
        (should-not staged-value)
        (should (buffer-live-p edit-buf))))))

(ert-deftest clutch-test-edit-finish-validates-enum-before-stage ()
  "Edit staging should reject invalid enum values locally."
  (let (staged-value err)
    (clutch-test--with-result-edit-buffer _edit-buf "urgent"
      (setq-local clutch-result-edit--column-name "severity"
                  clutch-result-edit--column-def '(:name "severity" :type-category text)
                  clutch-result-edit--column-detail
                  '(:name "severity" :type "enum('low','medium','high')")
                  clutch-result--edit-callback (lambda (value) (setq staged-value value)))
      (setq err (should-error (clutch-result-edit-finish) :type 'user-error))
      (should (string-match-p "Field severity must be one of: low, medium, high"
                              (error-message-string err)))
      (should-not staged-value))))

(ert-deftest clutch-test-edit-finish-validates-json-before-stage ()
  "Inline JSON edits should validate before staging."
  (let (staged-value err)
    (clutch-test--with-result-edit-buffer _edit-buf "{oops}"
      (setq-local clutch-result-edit--column-name "payload"
                  clutch-result-edit--column-def '(:name "payload" :type-category json)
                  clutch-result-edit--column-detail '(:name "payload" :type "json")
                  clutch-result--edit-callback (lambda (value) (setq staged-value value)))
      (setq err (should-error (clutch-result-edit-finish) :type 'user-error))
      (should (string-match-p "Field payload expects valid JSON"
                              (error-message-string err)))
      (should-not staged-value))))

(ert-deftest clutch-test-edit-set-null-stages-nil ()
  "The explicit NULL command should stage a nil value."
  (let ((staged-value :not-called)
        quit-called)
    (clutch-test--with-result-edit-buffer _edit-buf "12.5"
      (setq-local clutch-result-edit--column-name "impact_score"
                  clutch-result-edit--column-def '(:name "impact_score" :type-category numeric)
                  clutch-result-edit--column-detail '(:name "impact_score" :type "decimal(5,1)")
                  clutch-result--edit-callback
                  (lambda (value) (setq staged-value (list value))))
      (cl-letf (((symbol-function 'quit-window)
                 (lambda (&rest _args) (setq quit-called t))))
        (clutch-result-edit-set-null)
        (clutch-result-edit-finish)
        (should quit-called)
        (should (equal staged-value '(nil)))))))

(ert-deftest clutch-test-edit-finish-preserves-literal-null-string ()
  "Typing NULL should stage literal text, not database NULL."
  (let (staged-value)
    (clutch-test--with-result-edit-buffer _edit-buf "NULL"
      (setq-local clutch-result-edit--column-name "note"
                  clutch-result-edit--column-def '(:name "note" :type-category text)
                  clutch-result-edit--column-detail '(:name "note" :type "text")
                  clutch-result--edit-callback (lambda (value) (setq staged-value value)))
      (cl-letf (((symbol-function 'quit-window)
                 (lambda (&rest _args))))
        (clutch-result-edit-finish)
        (should (equal staged-value "NULL"))))))

(ert-deftest clutch-test-edit-finish-restores-result-cell-position ()
  "Finishing a cell edit should return point to the edited result cell."
  (save-window-excursion
    (let ((result-buf (generate-new-buffer "*clutch-result-test*"))
          edit-buf)
      (unwind-protect
          (progn
            (switch-to-buffer result-buf)
            (clutch-test--setup-rendered-result)
            (setq-local clutch-connection nil
                        clutch--connection-params '(:backend mysql)
                        clutch--result-source-table "users")
            (clutch--goto-cell 1 1)
            (cl-letf (((symbol-function 'clutch--ensure-column-details)
                       (lambda (&rest _) nil)))
              (clutch-result-edit-cell))
            (setq edit-buf (current-buffer))
            (erase-buffer)
            (insert "beta")
            (clutch-result-edit-finish)
            (should (eq (current-buffer) result-buf))
            (should (= (get-text-property (point) 'clutch-row-idx) 1))
            (should (= (get-text-property (point) 'clutch-col-idx) 1)))
        (when (buffer-live-p result-buf)
          (kill-buffer result-buf))
        (when (buffer-live-p edit-buf)
          (kill-buffer edit-buf))))))

(ert-deftest clutch-test-edit-finish-errors-when-result-buffer-is-dead ()
  "Finishing an edit should fail cleanly when the parent result buffer is gone."
  (let ((result-buf (generate-new-buffer "*clutch-result-test*"))
        edit-buf)
    (unwind-protect
        (with-current-buffer result-buf
          (clutch-result-mode)
          (setq-local clutch--result-columns '("name")
                      clutch--connection-params '(:backend mysql)
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

(ert-deftest clutch-test-insert-commit-validates-fields-before-stage ()
  "Insert staging should reject invalid enum, bool, JSON, temporal, and numeric values."
  (dolist (case
           `((("severity" "is_ship_blocked" "postmortem")
              ((:name "severity" :type-category text)
               (:name "is_ship_blocked" :type-category numeric)
               (:name "postmortem" :type-category json))
              "severity: nope\nis_ship_blocked: 7\npostmortem: not-json\n"
              ((:name "severity" :type "enum('low','medium')")
               (:name "is_ship_blocked" :type "tinyint(1)")
               (:name "postmortem" :type "json"))
              nil)
             (("opened_at" "due_on" "starts_at")
              ((:name "opened_at" :type-category datetime)
               (:name "due_on" :type-category date)
               (:name "starts_at" :type-category time))
              "opened_at: ss\ndue_on: 2026-02-30\nstarts_at: 25:61\n"
              ((:name "opened_at" :type "datetime")
               (:name "due_on" :type "date")
               (:name "starts_at" :type "time"))
              "Field opened_at expects YYYY-MM-DD HH:MM\\[:SS\\]")
             (("impact_score")
              ((:name "impact_score" :type-category numeric))
              "impact_score: xx\n"
              ((:name "impact_score" :type "decimal(5,1)"))
              "Field impact_score expects a numeric value")))
    (pcase-let ((`(,columns ,column-defs ,input ,details ,expected-message) case))
      (clutch-test--with-insert-result-buffer
          (result-buf columns column-defs 'clutch--pending-inserts nil)
        (with-temp-buffer
          (insert input)
          (clutch-result-insert-mode 1)
          (setq-local clutch-result-insert--result-buffer result-buf
                      clutch-result-insert--table "shipping_incidents")
          (cl-letf (((symbol-function 'clutch--ensure-column-details)
                     (lambda (_conn _table) details)))
            (let ((err (should-error (clutch-result-insert-commit)
                                     :type 'user-error)))
              (when expected-message
                (should (string-match-p expected-message
                                        (error-message-string err)))))))
        (should (buffer-live-p result-buf))
        (should-not (with-current-buffer result-buf clutch--pending-inserts))))))

;;;; Edit — JSON sub-editor

(ert-deftest clutch-test-json-editor-mode-uses-js-mode-without-json-ts-grammar ()
  "JSON editors should use `js-mode' when the tree-sitter grammar is unavailable."
  (let (selected-mode)
    (with-temp-buffer
      (cl-letf (((symbol-function 'json-ts-mode)
                 (lambda () (ert-fail "json-ts-mode should not run without a JSON grammar")))
                ((symbol-function 'treesit-language-available-p)
                 (lambda (_language) nil))
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
                            clutch--connection-params '(:backend mysql)
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

(ert-deftest clutch-test-document-result-export-formats-hide-sql-output ()
  "Native document results should expose document export choices, not SQL ones."
  (clutch-test--with-native-document-result-buffer
    (cl-letf (((symbol-function 'clutch-db-document-mutation-supported-p)
               (lambda (_conn action) (eq action 'insert-many))))
      (should (equal (mapcar #'car (clutch-result--available-export-formats))
                     '("csv-copy" "csv-file"
                       "document-insert-many-copy"
                       "document-insert-many-file"))))))

(ert-deftest clutch-test-sql-result-export-formats-hide-document-output ()
  "SQL results should expose SQL export choices, not document mutation choices."
  (with-temp-buffer
    (setq-local clutch-connection 'sql-conn
                clutch--connection-params nil)
    (clutch-test--with-connection-data-model ('sql-conn 'mysql 'relational)
      (should (equal (mapcar #'car (clutch-result--available-export-formats))
                     '("csv-copy" "csv-file"
                       "insert-copy" "insert-file"
                       "update-copy" "update-file"))))))

(ert-deftest clutch-test-key-value-result-export-formats-hide-sql-output ()
  "Key/value results should expose neutral exports, not SQL mutation output."
  (with-temp-buffer
    (setq-local clutch-connection 'redis-conn
                clutch--connection-params nil)
    (clutch-test--with-connection-data-model ('redis-conn 'redis 'key-value)
      (should (equal (mapcar #'car (clutch-result--available-export-formats))
                     '("csv-copy" "csv-file"))))))

(ert-deftest clutch-test-document-copy-uses-backend-mutation-snippet-generic ()
  "Document helper copy should use backend-owned mutation snippet generation."
  (clutch-test--with-native-document-result-buffer
    (let ((doc '(("_id" . 7) ("name" . "Ann")))
          captured
          kill-ring
          kill-ring-yank-pointer)
      (setq-local clutch--result-source-table "users"
                  clutch--result-columns '("_id" "name" "clutch__document")
                  clutch--result-column-defs
                  '((:name "_id" :type-category numeric)
                    (:name "name" :type-category text)
                    (:name "clutch__document"
                     :type-category json
                     :hidden t
                     :document-source t))
                  clutch--result-rows (list (list 7 "Ann" doc)))
      (cl-letf (((symbol-function 'clutch-db-document-mutation-supported-p)
                 (lambda (_conn action) (eq action 'update-one-set)))
                ((symbol-function 'clutch-db-document-mutation-snippets)
                 (lambda (conn action collection documents &optional fields)
                   (setq captured
                         (list conn action collection documents fields))
                   '("doc.update.snippet();")))
                ((symbol-function 'use-region-p) (lambda () nil))
                ((symbol-function 'clutch--cell-at-point)
                 (lambda () '(0 1 "Ann"))))
        (clutch-result-copy 'document-update-one-set)
        (should (equal captured
                       (list 'document-conn
                             'update-one-set
                             "users"
                             (list doc)
                             '("name"))))
        (should (equal (current-kill 0) "doc.update.snippet();"))))))

(ert-deftest clutch-test-document-result-rejects-sql-copy-format ()
  "Native document results should reject SQL copy formats before SQL building."
  (clutch-test--with-native-document-result-buffer
    (let ((err (should-error (clutch-result-copy 'insert)
                             :type 'user-error)))
      (should (string-match-p "Copy INSERT SQL is SQL-only"
                              (error-message-string err))))))

(ert-deftest clutch-test-key-value-result-rejects-sql-copy-format ()
  "Key/value results should reject SQL copy formats before SQL building."
  (with-temp-buffer
    (setq-local clutch-connection 'redis-conn
                clutch--connection-params nil)
    (clutch-test--with-connection-data-model ('redis-conn 'redis 'key-value)
      (let ((err (should-error (clutch-result-copy 'insert)
                               :type 'user-error)))
        (should (string-match-p "Copy INSERT SQL is SQL-only"
                                (error-message-string err)))))))

(ert-deftest clutch-test-document-result-rejects-sql-staged-edit ()
  "Native document results should reject SQL staged edit commands."
  (clutch-test--with-native-document-result-buffer
    (let ((err (should-error (clutch-result-edit-cell)
                             :type 'user-error)))
      (should (string-match-p "Edit / re-edit is SQL-only"
                              (error-message-string err))))))

(ert-deftest clutch-test-key-value-result-rejects-sql-staged-edit ()
  "Key/value results should reject SQL staged edit commands."
  (with-temp-buffer
    (setq-local clutch-connection 'redis-conn
                clutch--connection-params nil)
    (clutch-test--with-connection-data-model ('redis-conn 'redis 'key-value)
      (let ((err (should-error (clutch-result-edit-cell)
                               :type 'user-error)))
        (should (string-match-p "Edit / re-edit is SQL-only"
                                (error-message-string err)))))))

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

(ert-deftest clutch-test-result-mouse-click-below-table-preserves-point ()
  "Clicking below the rendered table should not move the current cell."
  (should (eq (lookup-key clutch-result-mode-map [mouse-1])
              #'clutch-result-mouse-set-point))
  (should (eq (lookup-key clutch-result-mode-map [down-mouse-1])
              #'clutch-result-mouse-set-point))
  (save-window-excursion
    (with-temp-buffer
      (switch-to-buffer (current-buffer))
      (clutch-result-mode)
      (let ((inhibit-read-only t))
        (insert "row\n"))
      (goto-char 2)
      (let ((event-position (point-max))
            delegated)
        (cl-letf (((symbol-function 'event-start) (lambda (_event) 'fake-posn))
                  ((symbol-function 'posn-window)
                   (lambda (_posn) (selected-window)))
                  ((symbol-function 'posn-point)
                   (lambda (_posn) event-position))
                  ((symbol-function 'mouse-drag-region)
                   (lambda (_event) (setq delegated 'drag)))
                  ((symbol-function 'mouse-set-point)
                   (lambda (_event) (setq delegated 'set-point))))
          (clutch-result-mouse-set-point 'mouse-1)
          (should (= (point) 2))
          (should-not delegated)
          (setq event-position 1)
          (clutch-result-mouse-set-point 'down-mouse-1)
          (should (eq delegated 'drag))
          (clutch-result-mouse-set-point 'mouse-1)
          (should (eq delegated 'set-point)))))))

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
      (setq-local clutch--connection-params '(:backend mysql))
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
    (setq-local clutch--connection-params '(:backend mysql))
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
    (insert "name")
    (add-text-properties (point-min) (point-max) '(clutch-col-idx 1))
    (goto-char (point-min))
    (setq-local clutch--result-columns '("id" "name" "age")
                clutch--result-column-defs '((:name "id")
                                             (:name "name")
                                             (:name "age"))
                clutch--result-server-rewritable t
                clutch--sort-column "name"
                clutch--sort-descending nil)
    (let (sort-args)
      (cl-letf (((symbol-function 'completing-read)
                 (lambda (&rest _) (error "unexpected sort column prompt")))
                ((symbol-function 'clutch-result--sort)
                 (lambda (col desc) (setq sort-args (list col desc)))))
        (clutch-result-sort-by-column)
        (should (equal sort-args '("name" t)))))))

(ert-deftest clutch-test-sort-by-column-clears-after-descending ()
  "Sorting the same DESC column again should clear SQL ORDER BY."
  (with-temp-buffer
    (insert "name")
    (add-text-properties (point-min) (point-max) '(clutch-col-idx 1))
    (goto-char (point-min))
    (setq-local clutch--result-columns '("id" "name" "age")
                clutch--result-column-defs '((:name "id")
                                             (:name "name")
                                             (:name "age"))
                clutch--result-server-rewritable t
                clutch--sort-column "name"
                clutch--sort-descending t
                clutch--order-by '("name" . "DESC")
                clutch--page-current 4)
    (let (pages sort-args)
      (cl-letf (((symbol-function 'completing-read)
                 (lambda (&rest _) (error "unexpected sort column prompt")))
                ((symbol-function 'clutch-result--sort)
                 (lambda (col desc) (setq sort-args (list col desc))))
                ((symbol-function 'clutch-result--execute-page)
                 (lambda (page &rest _)
                   (push page pages))))
        (clutch-result-sort-by-column)
        (should-not clutch--sort-column)
        (should-not clutch--sort-descending)
        (should-not clutch--order-by)
        (should (= clutch--page-current 0))
        (should (equal pages '(0)))
        (should-not sort-args)))))

(ert-deftest clutch-test-sort-by-column-new-column-defaults-ascending ()
  "Sorting a different column should default to ascending."
  (with-temp-buffer
    (insert "age")
    (add-text-properties (point-min) (point-max) '(clutch-col-idx 2))
    (goto-char (point-min))
    (setq-local clutch--result-columns '("id" "name" "age")
                clutch--result-column-defs '((:name "id")
                                             (:name "name")
                                             (:name "age"))
                clutch--result-server-rewritable t
                clutch--sort-column "name"
                clutch--sort-descending t)
    (let (sort-args)
      (cl-letf (((symbol-function 'completing-read)
                 (lambda (&rest _) (error "unexpected sort column prompt")))
                ((symbol-function 'clutch-result--sort)
                 (lambda (col desc) (setq sort-args (list col desc)))))
        (clutch-result-sort-by-column)
        (should (equal sort-args '("age" nil)))))))

(ert-deftest clutch-test-sort-by-column-errors-without-column-at-point ()
  "Keyboard sorting should require point to be on a result column."
  (with-temp-buffer
    (setq-local clutch--result-columns '("id" "name")
                clutch--result-column-defs '((:name "id") (:name "name")))
    (let ((err (should-error (clutch-result-sort-by-column)
                             :type 'user-error)))
      (should (string-match-p "No column at point"
                              (error-message-string err))))))

(ert-deftest clutch-test-auto-commit-transient-description-shows-state ()
  "Auto-commit transient label should show manual and automatic states."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-connection)
    (cl-letf (((symbol-function 'clutch--dispatch-transaction-controls-inapt-p)
               (lambda () nil)))
      (dolist (case '((t manual) (nil auto)))
        (pcase-let ((`(,manual-p ,active) case))
          (cl-letf (((symbol-function 'clutch-db-manual-commit-p)
                     (lambda (_connection) manual-p)))
            (let ((description (clutch--dispatch-auto-commit-description)))
              (should (equal (substring-no-properties description)
                             "Auto-commit (manual|auto)"))
              (let ((case-fold-search nil))
                (should (eq (get-text-property
                             (string-match (symbol-name active) description)
                             'face description)
                            'transient-value))))))))))

(ert-deftest clutch-test-copy-refine-infix-display-follows-switch-value ()
  "Copy refinement should display the active switch object value."
  (let* ((suffixes (transient-suffixes 'clutch-result-copy-dispatch))
         (refine
          (cl-find-if (lambda (obj)
                        (and (slot-boundp obj 'key)
                             (equal (oref obj key) "-r")))
                      suffixes)))
    (should refine)
    (should (equal (substring-no-properties
                    (transient-format-description refine))
                   "Refine selection"))
    (let ((value (transient-format-value refine)))
      (should (equal (substring-no-properties value) "(No|Yes)"))
      (should (eq (get-text-property (string-match "No" value)
                                     'face value)
                  'transient-value)))
    (let ((transient--prefix (get 'clutch-result-copy-dispatch
                                  'transient--prefix))
          (transient--suffixes suffixes))
      (cl-letf (((symbol-function 'transient--show) #'ignore))
        (transient-infix-set refine (transient-infix-read refine))))
    (let ((value (transient-format-value refine)))
      (should (equal (substring-no-properties value) "(No|Yes)"))
      (should (eq (get-text-property (string-match "Yes" value)
                                     'face value)
                  'transient-value)))))

(ert-deftest clutch-test-staged-transient-heading-shows-pending-count ()
  "Staged transient heading should summarize pending mutation count."
  (with-temp-buffer
    (setq-local clutch--pending-edits '(edit-a edit-b)
                clutch--pending-deletes '(delete-a)
                clutch--pending-inserts '(insert-a insert-b insert-c))
    (let ((heading (clutch-result--staged-transient-heading)))
      (should (equal (substring-no-properties heading) "Staged (6 pending)"))
      (should (eq (get-text-property (string-match "6 pending" heading)
                                    'face heading)
                  'warning)))
    (setq-local clutch--pending-edits nil
                clutch--pending-deletes nil
                clutch--pending-inserts nil)
    (should (equal (clutch-result--staged-transient-heading) "Staged"))))

(ert-deftest clutch-test-filter-transient-descriptions-show-current-values ()
  "Result filter labels should expose inactive and active values."
  (with-temp-buffer
    (should (equal (substring-no-properties
                    (clutch-result--client-filter-transient-description))
                   "Client filter (none|active)"))
    (setq-local clutch--filter-pattern "alice"
                clutch--where-filter "age > 18")
    (should (equal (substring-no-properties
                    (clutch-result--client-filter-transient-description))
                   "Client filter (none|active) [alice]"))
    (should (equal (substring-no-properties
                    (clutch-result--where-filter-transient-description))
                   "WHERE filter (none|active) [age > 18]"))))

(ert-deftest clutch-test-fullscreen-transient-description-shows-layout-state ()
  "Result layout label should expose window and fullscreen states."
  (with-temp-buffer
    (setq-local clutch--pre-fullscreen-config nil)
    (should (equal (substring-no-properties
                    (clutch-result--fullscreen-transient-description))
                   "Layout (window|fullscreen)"))
    (setq-local clutch--pre-fullscreen-config 'saved-configuration)
    (let ((description (clutch-result--fullscreen-transient-description)))
      (should (equal (substring-no-properties description)
                     "Layout (window|fullscreen)"))
      (should (eq (get-text-property (string-match "fullscreen" description)
                                    'face description)
                  'transient-value)))))

(ert-deftest clutch-test-sort-transient-description-shows-three-state ()
  "Result sort transient description should show one three-state control."
  (with-temp-buffer
    (insert "created_at")
    (add-text-properties (point-min) (point-max) '(clutch-col-idx 0))
    (goto-char (point-min))
    (setq-local clutch--result-columns '("created_at")
                clutch--result-column-defs '((:name "created_at")))
    (let ((desc (clutch-result--sort-transient-description)))
      (should (string-match-p "Sort current" desc))
      (should (string-match-p "(none|asc|desc)" desc))
      (should (string-match-p "\\[created_at\\]" desc))
      (should (eq (get-text-property (string-match "none" desc) 'face desc)
                  'transient-value)))
    (setq-local clutch--sort-column "created_at"
                clutch--sort-descending t)
    (let ((desc (clutch-result--sort-transient-description)))
      (should (eq (get-text-property (string-match "desc" desc) 'face desc)
                  'transient-value)))
    (remove-text-properties (point-min) (point-max) '(clutch-col-idx nil))
    (should (equal (substring-no-properties
                    (clutch-result--sort-transient-description))
                   "Sort current (no column)"))))

(ert-deftest clutch-test-sort-transient-description-targets-point-column ()
  "Result sort transient description should describe the current-column target."
  (with-temp-buffer
    (insert "age")
    (add-text-properties (point-min) (point-max) '(clutch-col-idx 2))
    (goto-char (point-min))
    (setq-local clutch--result-columns '("id" "name" "age")
                clutch--result-column-defs '((:name "id")
                                             (:name "name")
                                             (:name "age"))
                clutch--sort-column "name"
                clutch--sort-descending t)
    (let ((desc (substring-no-properties
                 (clutch-result--sort-transient-description))))
      (should (string-match-p "Sort current" desc))
      (should (string-match-p "\\[age\\]" desc))
      (should (string-match-p "(none|asc|desc)" desc)))))

(ert-deftest clutch-test-sort-by-header-column-uses-captured-visible-name ()
  "Header sorting should tolerate a stale index when the captured name is valid."
  (with-temp-buffer
    (setq-local clutch--result-server-rewritable t
                clutch--result-server-pageable t
                clutch--result-columns '("id" "name"))
    (let (pages)
      (cl-letf (((symbol-function 'clutch-result--execute-page)
                 (lambda (page &rest _)
                   (push page pages))))
        (clutch-result--sort-by-column-index 99 "name")
        (should (equal clutch--sort-column "name"))
        (should (equal clutch--order-by '("name" . "ASC")))
        (should (equal pages '(0)))))))

(ert-deftest clutch-test-sort-by-header-column-rejects-stale-name ()
  "Header sorting should not fall back to another column at the same index."
  (with-temp-buffer
    (setq-local clutch--result-columns '("id" "age")
                clutch--result-column-defs '((:name "id") (:name "age")))
    (let (sort-args)
      (cl-letf (((symbol-function 'clutch-result--sort)
                 (lambda (name descending)
                   (setq sort-args (list name descending)))))
        (let ((err (should-error
                    (clutch-result--sort-by-column-index 1 "name")
                    :type 'user-error)))
          (should (string-match-p "Column not found"
                                  (error-message-string err)))))
      (should-not sort-args))))

(ert-deftest clutch-test-sort-by-header-column-cycles-state ()
  "Click-style header sorting should cycle ASC, DESC, then unsorted."
  (with-temp-buffer
    (setq-local clutch--result-server-rewritable t
                clutch--result-server-pageable t
                clutch--result-columns '("id" "name")
                clutch--page-current 3)
    (let (pages)
      (cl-letf (((symbol-function 'clutch-result--execute-page)
                 (lambda (page &rest _)
                   (push page pages))))
        (clutch-result--sort-by-column-index 1)
        (should (equal clutch--sort-column "name"))
        (should-not clutch--sort-descending)
        (should (equal clutch--order-by '("name" . "ASC")))
        (should (= clutch--page-current 0))
        (clutch-result--sort-by-column-index 1)
        (should (equal clutch--sort-column "name"))
        (should clutch--sort-descending)
        (should (equal clutch--order-by '("name" . "DESC")))
        (clutch-result--sort-by-column-index 1)
        (should-not clutch--sort-column)
        (should-not clutch--sort-descending)
        (should-not clutch--order-by)
        (clutch-result--sort-by-column-index 0)
        (should (equal clutch--sort-column "id"))
        (should-not clutch--sort-descending)
        (should (equal clutch--order-by '("id" . "ASC")))
        (should (equal (nreverse pages) '(0 0 0 0)))))))

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

(ert-deftest clutch-test-execute-db-error-abandons-dead-connection ()
  "Query errors should clear connection state when the backend closed it."
  (with-temp-buffer
    (let* ((conn 'fake-conn)
           (clutch-connection conn)
           (clutch--tx-dirty-cache (make-hash-table :test 'eq))
           (clutch--executing-p nil)
           (displayed-error nil)
           (mode-line-updates 0))
      (puthash conn t clutch--tx-dirty-cache)
      (cl-letf (((symbol-function 'clutch--ensure-connection) #'ignore)
                ((symbol-function 'clutch-result--check-pending-changes) #'ignore)
                ((symbol-function 'clutch-db-sql-destructive-p)
                 (lambda (_sql) nil))
                ((symbol-function 'clutch-db-result-query-p)
                 (lambda (_conn _sql) t))
                ((symbol-function 'clutch-db-query-result-context)
                 (lambda (&rest _args) nil))
                ((symbol-function 'clutch--prepare-row-identity-query)
                 (lambda (&rest _args)
                   (list :sql "SELECT SLEEP(60)")))
                ((symbol-function 'clutch--sql-has-page-tail-p)
                 (lambda (_sql) t))
                ((symbol-function 'clutch--connection-alive-p)
                 (lambda (_conn) nil))
                ((symbol-function 'clutch--run-db-query)
                 (lambda (&rest _args)
                   (signal 'clutch-db-error '("query timed out"))))
                ((symbol-function 'clutch--show-execution-error)
                 (lambda (_source _conn _sql err &optional _elapsed _context _region)
                   (setq displayed-error (error-message-string err))))
                ((symbol-function 'clutch--update-mode-line)
                 (lambda ()
                   (setq mode-line-updates (1+ mode-line-updates)))))
        (should-not (clutch--execute "SELECT SLEEP(60)" conn))
        (should (string-match-p "query timed out" displayed-error))
        (should-not clutch-connection)
        (should-not (gethash conn clutch--tx-dirty-cache))
        (should-not clutch--executing-p)
        (should (> mode-line-updates 0))))))

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

(ert-deftest clutch-test-execute-uses-backend-result-query-p ()
  "Execute should let the backend classify non-SQL result-set queries."
  (let ((clutch-connection 'document-conn)
        captured)
    (cl-letf (((symbol-function 'clutch--ensure-connection) (lambda () t))
              ((symbol-function 'clutch-result--check-pending-changes) #'ignore)
              ((symbol-function 'clutch-db-sql-destructive-p) (lambda (_sql) nil))
              ((symbol-function 'clutch--require-risky-dml-confirmation) #'ignore)
              ((symbol-function 'clutch--spinner-start) #'ignore)
              ((symbol-function 'clutch--update-mode-line) #'ignore)
              ((symbol-function 'clutch-db-result-query-p)
               (lambda (conn sql)
                 (setq captured (list conn sql))
                 t))
              ((symbol-function 'clutch--execute-select)
               (lambda (_sql _conn &optional _result-context) 'ok))
              ((symbol-function 'clutch--execute-dml)
               (lambda (&rest _args)
                 (ert-fail "Document result query should not use DML path"))))
      (clutch--execute "db.users.find()" clutch-connection)
      (should (equal captured '(document-conn "db.users.find()"))))))

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

(provide 'clutch-test)

;;; clutch-test.el ends here
