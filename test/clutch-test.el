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
;;   emacs --batch -Q -L . -L test -L ../mysql.el -L ~/repos/pgsql.el \
;;     --eval '(setq load-prefer-newer t)' \
;;     -l ert -l clutch-test \
;;     -f ert-run-tests-batch-and-exit

;;; Code:

(eval-and-compile
  (require 'clutch-test-common)
  (require 'clutch-db-sqlite))

;;;; Test configuration

(defvar clutch-column-displayers)

(defvar clutch--result-source-table)

(defvar clutch--connection-params)

(defvar clutch--result-server-pageable)

(defvar clutch--result-server-rewritable)

(defvar clutch--local-sort-original-rows)

(defvar clutch--local-sort-column-index)

(defvar tramp-rpc-use-controlmaster)

(declare-function make-clutch-jdbc-conn "clutch-db-jdbc" (&rest slot-value-pairs))

(declare-function make-mysql-conn "mysql" (&rest args))

(declare-function clutch-db-pg--type-category "clutch-db-pg" (oid))

(declare-function clutch-db-pg--make-connection "clutch-db-pg" (&rest args))

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

(defun clutch-test--transient-suffix-for-key (prefix key)
  "Return the suffix under PREFIX bound to KEY."
  (cl-find-if
   (lambda (suffix)
     (and (slot-boundp suffix 'key)
          (equal (oref suffix key) key)))
   (transient-suffixes prefix)))

(require 'clutch-test-sql)
(require 'clutch-test-console)
(require 'clutch-test-object)
(require 'clutch-test-connection)
(require 'clutch-test-debug)
(require 'clutch-test-backends)
(require 'clutch-document)

;;;; Test backend matrix

(ert-deftest clutch-test-backend-matrix-selects-live-workflow-capabilities ()
  :tags '(:smoke)
  "Live backend matrix should replace hard-coded workflow backend lists."
  (let ((clutch-test-backend 'jdbc)
        (clutch-test-url "jdbc:duckdb:/tmp/clutch-test.duckdb"))
    (should (eq (clutch-test-live-backend-id) 'duckdb))
    (should (clutch-test-live-backend-capability-p :result-workflow))
    (should (clutch-test-live-backend-capability-p :updateable-workflow))
    (should-not
     (clutch-test-live-backend-capability-p :manual-savepoint)))
  (let ((clutch-test-backend 'clickhouse)
        (clutch-test-url nil))
    (should (eq (clutch-test-live-backend-id) 'clickhouse))
    (should (clutch-test-live-backend-capability-p :result-workflow))
    (should-not
     (clutch-test-live-backend-capability-p :updateable-workflow)))
  (should (clutch-test-live-backend-capability-p :object-describe 'mysql))
  (should (clutch-test-live-backend-capability-p :ctid-row-identity 'pg))
  (dolist (backend '(mysql pg sqlserver oracle))
    (should
     (clutch-test-live-backend-capability-p :manual-savepoint backend)))
  (should-not
   (clutch-test-live-backend-capability-p :manual-savepoint 'jdbc))
  (should-not (clutch-test-live-backend-capability-p :result-workflow
                                                     'mongodb))
  (should (string-match-p
           "MySQL/PostgreSQL"
           (clutch-test-capability-skip-message :object-describe))))

;;;; Rendering — value formatting

(ert-deftest clutch-test-format-value-values ()
  :tags '(:smoke)
  "Result values should render as compact display strings."
  (dolist (case '((nil "NULL")
                  (:false "false")
                  (:null "null")
                  ("hello" "hello")
                  ("" "")
                  (42 "42")
                  (-1 "-1")
                  (3.14 "3.14")
                  ([1 2 3] "[1,2,3]")
                  ([1 nil 3] "[1,null,3]")
                  ([[1 nil] [3 4]] "[[1,null],[3,4]]")
                  ([#s(hash-table test equal data ("a" :null)) nil]
                   "[{\"a\":null},null]")))
    (pcase-let ((`(,value ,expected) case))
      (should (equal (clutch--format-value value) expected))))
  (should (equal (clutch--format-value '(:year 2024 :month 3 :day 15))
                 "2024-03-15"))
  (should (equal (clutch--format-value
                  '(:hours 13 :minutes 45 :seconds 30 :negative nil))
                 "13:45:30"))
  (should (equal (clutch--format-value
                  '(:year 2024 :month 3 :day 15
                    :hours 13 :minutes 45 :seconds 30))
                 "2024-03-15 13:45:30")))

(ert-deftest clutch-test-format-value-json-hash-table ()
  "Parsed JSON objects should render as compact JSON strings."
  (let ((ht (make-hash-table :test 'equal)))
    (puthash "key" "val" ht)
    (let ((result (clutch--format-value ht)))
      (should (stringp result))
      (should (string-match-p "\"key\"" result))
      (should (string-match-p "\"val\"" result)))))

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

(ert-deftest clutch-test-value-to-literal-json-values ()
  "JSON objects and arrays should become quoted SQL string literals."
  (let* ((ht (make-hash-table :test 'equal))
         (_ (puthash "k" "v" ht))
         (conn (make-clutch-jdbc-conn
                :params '(:driver sqlserver :user "sa"))))
    (dolist (case (list (list ht '("\"k\"" "\"v\""))
                        (list [1 2 3] '("1" "2"))))
      (pcase-let ((`(,value ,needles) case))
        (let ((result (clutch-db-value-to-literal
                       conn value #'clutch--format-value)))
          (should (stringp result))
          (dolist (needle needles)
            (should (string-match-p needle result))))))))

(ert-deftest clutch-test-json-value-to-string-values ()
  "JSON viewer values should serialize as valid, readable JSON."
  (let ((obj (make-hash-table :test 'equal)))
    (puthash "a" 1 obj)
    (should (equal (clutch--json-value-to-string obj) "{\"a\":1}")))
  (let ((obj (make-hash-table :test 'equal)))
    (puthash "quote" "记忆碎片已封存" obj)
    (puthash "operator" "Saito" obj)
    (puthash "thermoptic" :false obj)
    (should (equal (clutch--json-value-to-string obj)
                   "{\"quote\":\"记忆碎片已封存\",\"operator\":\"Saito\",\"thermoptic\":false}")))
  (dolist (case '(("hello" "\"hello\"")
                  (nil "null")
                  (t "true")
                  (:false "false")
                  (42 "42")))
    (pcase-let ((`(,value ,expected) case))
      (should (equal (clutch--json-value-to-string value) expected)))))

(ert-deftest clutch-test-dispatch-view-json-values ()
  "JSON dispatch should serialize non-strings and pass JSON strings through."
  (let (seen buffer-name)
    (cl-letf (((symbol-function 'clutch--view-in-buffer)
               (lambda (content name _setup)
                 (setq seen content
                       buffer-name name)))
              ((symbol-function 'clutch--json-value-to-string)
               (lambda (_val) "{\"ok\":true}")))
      (clutch--dispatch-view (vector 1 2) '(:type-category json))
      (should (equal seen "{\"ok\":true}"))
      (should (equal buffer-name "*clutch-json*"))))
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
  "JSON viewers should use `json-mode' without tree-sitter or its JSON grammar."
  (pcase-dolist (`(,label ,treesit ,grammar-fn)
                 `(("no JSON grammar" t ,(lambda (_language &optional _quiet) nil))
                   ;; An Emacs built without tree-sitter does not define
                   ;; `treesit-language-available-p' at all.
                   ("no tree-sitter" nil nil)))
    (ert-info (label)
      (let (selected-mode)
        (with-temp-buffer
          (insert "{\"ok\":true}")
          (cl-letf (((symbol-function 'json-ts-mode)
                     (lambda () (ert-fail "json-ts-mode should not run without a JSON grammar")))
                    ((symbol-function 'treesit-available-p) (lambda () treesit))
                    ((symbol-function 'treesit-language-available-p) grammar-fn)
                    ((symbol-function 'json-mode)
                     (lambda () (setq selected-mode 'json-mode)))
                    ((symbol-function 'js-mode)
                     (lambda () (setq selected-mode 'js-mode))))
            (clutch--setup-json-view-buffer)))
        (should (eq selected-mode 'json-mode))))))

(ert-deftest clutch-test-dispatch-view-routes-values-by-content ()
  "Value viewers should choose JSON/XML/plain buffers from type and content."
  (dolist (case `(("hello" (:type-category text) "*clutch-value*" "hello" nil)
                  ("{not json" (:type-category text) "*clutch-value*" "{not json" nil)
                  (nil (:type-category json) "*clutch-value*"
                       ,clutch--null-cell-display-text clutch-null-face)
                  ("<rss><item>1</item></rss>"
                   (:type-category blob) "*clutch-xml*"
                   "<rss><item>1</item></rss>" nil)
                  ("<abc" (:type-category text) "*clutch-value*" "<abc" nil)))
    (pcase-let ((`(,value ,column ,expected-buffer ,expected-content
                          ,expected-face)
                 case))
      (let (buffer-name content)
        (cl-letf (((symbol-function 'clutch--view-in-buffer)
                   (lambda (text name _setup)
                     (setq content text
                           buffer-name name))))
          (clutch--dispatch-view value column)
          (should (equal content expected-content))
          (when expected-face
            (should (text-property-any 0 (length content)
                                       'face expected-face content)))
          (should (equal buffer-name expected-buffer)))))))

(ert-deftest clutch-test-blob-view-string-previews ()
  "Blob preview should choose hex or text output from the value bytes."
  (dolist (case (list (list (unibyte-string #x00 #xff #x41 #x7f)
                            "BLOB size: 4 bytes"
                            '("Hex preview:" "00 ff 41 7f")
                            nil)
                      (list "hello world"
                            "BLOB size: 11 bytes"
                            '("Text preview:")
                            '("Hex preview:"))))
    (pcase-let ((`(,value ,size-line ,present ,absent) case))
      (let ((s (clutch--blob-view-string value)))
        (should (string-match-p size-line s))
        (dolist (needle present)
          (should (string-match-p needle s)))
        (dolist (needle absent)
          (should-not (string-match-p needle s)))))))

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
  (should (clutch--xml-like-string-p " \n<rss/>"))
  (should (clutch--json-like-string-p " \n{\"ok\": true}"))
  (should-not (clutch--xml-like-string-p "<abc"))
  (should-not (clutch--xml-like-string-p "just <text> marker")))

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

(ert-deftest clutch-test-value-to-literal-basic-values ()
  "Scalar values should become SQL literals."
  (dolist (case '((nil "NULL") (42 "42") (-1 "-1")))
    (pcase-let ((`(,value ,expected) case))
      (should (equal (clutch-db-value-to-literal 'fake-conn value)
                     expected))))
  (should (string-match-p "3\\.14"
                          (clutch-db-value-to-literal 'fake-conn 3.14)))
  (require 'clutch-db-mysql)
  (require 'mysql)
  (let ((conn (make-mysql-conn :host "localhost")))
    (let ((result (clutch-db-value-to-literal conn "hello")))
      (should (stringp result))
      (should (string-prefix-p "'" result)))
    (let ((result (clutch-db-value-to-literal conn "it's")))
      (should (string-match-p "\\\\'" result)))))

;;;; Rendering — padding

(defun clutch-test--fake-char-pixel-width (string pos)
  "Return deterministic pixels for the character at POS in STRING."
  (let ((display (get-text-property pos 'display string)))
    (cond
     ((eq (aref string pos) ?\n) 0)
     ((equal display "") 0)
     ((stringp display)
      (clutch-test--fake-pixel-width display))
     ((and (consp display) (eq (car display) 'space))
      (let ((width (plist-get (cdr display) :width)))
        (if (consp width) (car width) width)))
     ((and (consp display) (eq (car display) 'raise)) 25)
     ((zerop (char-width (aref string pos))) 0)
     ((memq (aref string pos) '(?中 ?文)) 30)
     (t 10))))

(defun clutch-test--fake-pixel-width (string)
  "Return deterministic mixed-width pixels for STRING."
  (let ((pos 0)
        (pixels 0))
    (while (< pos (length string))
      (if-let* ((min-width (get-display-property pos 'min-width string))
                (target (caar min-width))
                ((numberp target)))
          (let* ((end (next-single-property-change
                       pos 'display string (length string)))
                 (content-pixels
                  (cl-loop for i from pos below end
                           sum (clutch-test--fake-char-pixel-width string i))))
            (setq pixels (+ pixels (max target content-pixels))
                  pos end))
        (setq pixels (+ pixels
                        (clutch-test--fake-char-pixel-width string pos))
              pos (1+ pos))))
    pixels))

(ert-deftest clutch-test-result-pixel-padding-contract ()
  "Result cells should use exact graphical padding on every Emacs version."
  (cl-letf (((symbol-function 'default-font-width) (lambda () 10))
            ((symbol-function 'string-pixel-width)
             #'clutch-test--fake-pixel-width))
    (let ((left (clutch--pad-display-string "x" 4 50 nil 10))
          (right (clutch--pad-display-string "7" 4 50 t))
          (empty (clutch--pad-display-string "" 4 50)))
      (dolist (string (list left right empty))
        (should (= (string-width string) 4))
        (should (= (clutch-test--fake-pixel-width string) 50))
        (should-not
         (cl-loop for i below (length string)
                  thereis
                  (get-display-property i 'min-width string))))
      (should (equal (get-text-property 1 'display left)
                     '(space :width (40))))
      (should (equal (get-text-property 0 'display right)
                     '(space :width (40))))
      (should (equal (get-text-property 0 'display empty)
                     '(space :width (50)))))))

(ert-deftest clutch-test-header-pixel-padding-contract ()
  "Header centering should use exact pixel spaces."
  (cl-letf (((symbol-function 'default-font-width) (lambda () 10))
            ((symbol-function 'string-pixel-width)
             #'clutch-test--fake-pixel-width)
            ((symbol-function 'clutch--header-cell-label)
             (lambda (_cidx _width) "x")))
    (let* ((clutch--result-columns '("x"))
           (clutch--column-pixel-widths [50])
           (header (clutch--header-cell 0 [4])))
      (should-not
       (cl-loop for i below (length header)
                thereis
                (get-display-property i 'min-width header)))
      (should
       (cl-loop for i below (length header)
                thereis
                (equal (get-text-property i 'display header)
                       '(space :width (20))))))))

(ert-deftest clutch-test-pixel-measurement-applies-default-face-remapping ()
  "Pixel measurement should include the result buffer's text remapping."
  (let ((face-remapping-alist '((default (:height 2.0) default))))
    (cl-letf (((symbol-function 'string-pixel-width)
               (lambda (string)
                 (if (equal (get-text-property 0 'face string)
                            '(:height 2.0))
                     20
                   10))))
      (should (= (clutch--display-string-pixel-width "x") 20)))))

(ert-deftest clutch-test-pixel-metric-detects-wide-truncation-ellipsis ()
  "Pixel layout should detect an ellipsis wider than one logical cell."
  (cl-letf (((symbol-function 'display-graphic-p)
             (lambda (&optional _display) t))
            ((symbol-function 'default-font-width) (lambda () 10))
            ((symbol-function 'clutch--display-string-pixel-width)
             (lambda (string)
               (if (equal string "…")
                   20
                 (* 10 (string-width string))))))
    (should (clutch--pixel-metric-signature))))

(ert-deftest clutch-test-result-grid-aligns-mixed-width-custom-displays ()
  "Result headers and custom display subregions should share rendered widths."
  (let ((clutch-column-displayers nil)
        (clutch--column-displayer-version 0)
        (wide (copy-sequence "中文"))
        (narrow (copy-sequence "Ix")))
    (put-text-property 0 2 'display '(space :width (30)) wide)
    (put-text-property 0 1 'display '(raise 0.0) narrow)
    (clutch-register-column-displayer
     "items" "state"
     (lambda (value)
       (copy-sequence (if (string= value "wide") wide narrow))))
    (clutch-test--with-result-state
        (:source-table "items"
         :columns '("state")
         :column-defs '((:name "state"))
         :rows '(("wide") ("narrow"))
         :column-widths [5])
      (let (header rows)
        (cl-letf (((symbol-function 'display-graphic-p)
                   (lambda (&optional _display) t))
                  ((symbol-function 'default-font-width) (lambda () 1))
                  ((symbol-function 'clutch--pixel-metric-signature)
                   (lambda () '(mixed-width)))
                  ((symbol-function 'string-pixel-width)
                   #'clutch-test--fake-pixel-width)
                  ((symbol-function 'clutch--header-label)
                   (lambda (name _cidx)
                     (propertize name 'clutch-header-name t)))
                  ((symbol-function 'clutch--refresh-footer-line) #'ignore))
          (clutch--render-result)
          (setq header clutch--header-line-string
                rows (string-lines (string-trim-right (buffer-string))))
          (should (equal clutch--column-pixel-widths [60])))
        (dolist (string rows)
          (let* ((start
                  (cl-loop for i below (length string)
                           when (eq (get-text-property
                                     i 'clutch-col-idx string)
                                    0)
                           return i))
                 (end (next-single-property-change
                       start 'clutch-col-idx string (length string)))
                 (cell (substring string start end)))
            (should
             (= (clutch-test--fake-pixel-width cell)
                (+ (aref clutch--column-pixel-widths 0)
                   (* 2 clutch-column-padding
                      (clutch-test--fake-pixel-width " ")))))
            (should-not
             (cl-loop for i below (length cell)
                      thereis
                      (get-display-property i 'min-width cell))))
          (should (= (string-width header) (string-width string))))))))

(ert-deftest clutch-test-install-page-state-contract ()
  "New query results should discard values and preserve compatible font caches."
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
      (should-not clutch--cell-render-cache)
      (should (eq clutch--char-pixel-width-cache char-cache))
      (clutch-result--install-page-state columns '((3 "eve")) 0.1 1)
      (should-not clutch--cell-render-cache)
      (should (eq clutch--char-pixel-width-cache char-cache))
      (setq-local clutch--cell-render-cache cell-cache)
      (clutch-result--install-page-state '((:name "other")) '(("x")) 0.1 0)
      (should-not clutch--cell-render-cache)
      (should-not clutch--char-pixel-width-cache)))
  (dolist (case '((same ("id" "name")
                        ((:name "id" :type-category numeric)
                         (:name "name" :type-category text))
                        ((100 "a much longer customer name"))
                        [12 7])
                  (changed ("id" "email")
                           ((:name "id" :type-category numeric)
                            (:name "email" :type-category text))
                           ((100 "alice@example.test"))
                           nil)))
    (pcase-let ((`(,label ,expected-columns ,columns ,rows ,expected-widths)
                 case))
      (ert-info ((format "manual widths: %s" label))
        (with-temp-buffer
          (setq-local clutch--result-columns '("id" "name")
                      clutch--column-widths [12 7]
                      clutch-result-max-rows 50)
          (clutch-result--install-page-state columns rows 0.1 0)
          (should (equal clutch--result-columns expected-columns))
          (if expected-widths
              (should (equal clutch--column-widths expected-widths))
            (should-not (equal clutch--column-widths [12 7]))))))))

;;;; Rendering — column layout and widths

(ert-deftest clutch-test-compute-column-widths ()
  "Column width computation should handle base and typed display rules."
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
    (should (>= (aref widths 2) 5)))
  (dolist (case '(("max cap"
                   10 ("description")
                   (("this is a very long description that exceeds the maximum width"))
                   ((:name "description" :type-category text))
                   <= 10)
                  ("short JSON"
                   30 ("j") (("{\"a\":1}"))
                   ((:name "j" :type-category json))
                   = 7)
                  ("compact blob"
                   30 ("payload") (("this blob text is intentionally long"))
                   ((:name "payload" :type-category blob))
                   = 10)
                  ("structured blob"
                   30 ("payload") (("{\"a\":1,\"b\":2}"))
                   ((:name "payload" :type-category blob))
                   = 13)))
    (pcase-let ((`(,label ,max-width ,col-names ,rows ,columns
                          ,predicate ,expected)
                 case))
      (ert-info ((format "case: %s" label))
        (let* ((clutch-column-width-max max-width)
               (widths (clutch--compute-column-widths
                        col-names rows columns)))
          (should (funcall predicate (aref widths 0) expected)))))))

(ert-deftest clutch-test-render-result-aligns-short-null-columns-with-fallback-sort ()
  "Short NULL columns should not shift later columns when sort icons fall back."
  (let* ((columns '("hb" "party" "rh"))
         (rows '((nil 5 nil) (nil 6 nil)))
         (column-defs (mapcar (lambda (name) (list :name name)) columns))
         (clutch--header-sort-indicator-cache (make-hash-table :test 'equal)))
    (cl-letf (((symbol-function 'clutch--icon)
               (lambda (_spec fallback &rest _args) fallback)))
      (clutch-test--with-result-state
          (:columns columns
           :column-defs column-defs
           :rows rows
           :column-widths (clutch--compute-column-widths columns rows column-defs)
           :render t)
        (let* ((row (buffer-substring (line-beginning-position)
                                      (line-end-position)))
               (border-columns
                (lambda (string)
                  (cl-loop for idx below (length string)
                           when (= (aref string idx) ?│)
                           collect (string-width (substring string 0 idx))))))
          (should (equal (funcall border-columns clutch--header-line-string)
                         (funcall border-columns row))))))))

(ert-deftest clutch-test-visible-columns-contract ()
  "Visible column helpers should include user columns and skip hidden metadata."
  (with-temp-buffer
    (setq-local clutch--result-columns '("c1" "c2" "c3" "c4"))
    (should (equal (clutch--visible-columns) '(0 1 2 3))))
  (with-temp-buffer
    (setq-local clutch--result-columns '("clutch__rid_0" "id" "name")
                clutch--result-column-defs
                '((:name "clutch__rid_0" :hidden t)
                  (:name "id")
                  (:name "name")))
    (should (equal (clutch--visible-columns) '(1 2)))
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

(ert-deftest clutch-test-goto-column-centers-target-column ()
  "Column jumps should center the target column in the current window."
  (save-window-excursion
    (let ((buf (generate-new-buffer "*clutch-goto-column-test*")))
      (unwind-protect
          (progn
            (switch-to-buffer buf)
            (clutch-test--init-result-state
             (list :columns '("id" "name" "city" "note" "flag")
                   :rows '((1 "alpha" "oslo" "before" "x")
                           (2 "bravo" "rome" "target" "y"))
                   :page-total-rows 2
                   :column-widths [3 18 18 18 18]
                   :render t))
            (clutch--goto-cell 1 0)
            (let (hscroll)
              (cl-letf (((symbol-function 'completing-read)
                         (lambda (&rest _args) "flag"))
                        ((symbol-function 'get-buffer-window)
                         (lambda (&rest _args) (selected-window)))
                        ((symbol-function 'window-hscroll)
                         (lambda (&rest _args) (or hscroll 0)))
                        ((symbol-function 'window-body-width)
                         (lambda (&rest _args) 80))
                        ((symbol-function 'set-window-hscroll)
                         (lambda (_window value &optional _min)
                           (setq hscroll value))))
                (clutch-result-goto-column))
              (should (= (get-text-property (point) 'clutch-row-idx) 1))
              (should (= (get-text-property (point) 'clutch-col-idx) 4))
              (should (= hscroll 47))))
        (when (buffer-live-p buf)
          (kill-buffer buf))))))

(ert-deftest clutch-test-result-edge-column-navigation-keeps-current-row ()
  "Brace commands should reach visible edge columns without leaving the row."
  (clutch-test--with-result-state
      (:columns '("rid-left" "first" "last" "rid-right")
       :column-defs '((:name "rid-left" :hidden t)
                      (:name "first")
                      (:name "last")
                      (:name "rid-right" :hidden t))
       :rows '((1 "a" "b" 10) (2 "c" "d" 20))
       :column-widths [8 5 5 9]
       :render t)
    (should (eq (lookup-key clutch-result-mode-map "{")
                #'clutch-result-first-column))
    (should (eq (lookup-key clutch-result-mode-map "}")
                #'clutch-result-last-column))
    (goto-char (aref clutch--row-start-positions 1))
    (call-interactively (key-binding (kbd "{")))
    (should (= (get-text-property (point) 'clutch-row-idx) 1))
    (should (= (get-text-property (point) 'clutch-col-idx) 1))
    (end-of-line)
    (call-interactively (key-binding (kbd "}")))
    (should (= (get-text-property (point) 'clutch-row-idx) 1))
    (should (= (get-text-property (point) 'clutch-col-idx) 2))))

(ert-deftest clutch-test-result-mode-keeps-native-line-numbers-off ()
  "Result mode refreshes should retain ownership of the row-number gutter."
  (let ((global-was-on (bound-and-true-p global-display-line-numbers-mode)))
    (unwind-protect
        (with-temp-buffer
          (global-display-line-numbers-mode 1)
          (clutch-result-mode)
          (should-not display-line-numbers-mode)
          (should-not display-line-numbers)
          (display-line-numbers-mode 1)
          (clutch-result-mode)
          (should-not display-line-numbers-mode)
          (should-not display-line-numbers))
      (global-display-line-numbers-mode (if global-was-on 1 -1)))))

(ert-deftest clutch-test-row-identity-prep-augments-row-preserving-selects ()
  "Row-preserving SELECTs should receive hidden identity expressions."
  (cl-letf ((clutch--row-identity-cache (make-hash-table :test 'eq))
            ((symbol-function 'clutch-db-row-identity-candidates)
             (lambda (_conn _table)
               (list (list :kind 'primary-key
                           :name "PRIMARY"
                           :columns '("id")))))
            ((symbol-function 'clutch-db-escape-identifier)
             (lambda (_conn id) (format "\"%s\"" id))))
    (dolist (case
             '(("simple filter"
                "SELECT name FROM users WHERE active = 1"
                "SELECT name, \"id\" AS \"clutch__rid_0\" FROM users WHERE active = 1")
               ("leading comment"
                "-- comment\nSELECT * FROM users;"
                "SELECT users.*, \"id\" AS \"clutch__rid_0\" FROM users")
               ("window aggregate"
                "SELECT name, count(*) OVER () AS total FROM users"
                "SELECT name, count(*) OVER () AS total, \"id\" AS \"clutch__rid_0\" FROM users")
               ("filtered window aggregate"
                "SELECT name, sum(score) FILTER (WHERE score > 0) OVER () AS total FROM users"
                "SELECT name, sum(score) FILTER (WHERE score > 0) OVER () AS total, \"id\" AS \"clutch__rid_0\" FROM users")
               ("scalar subquery"
                "SELECT name, (SELECT count(*) FROM orders) AS order_count FROM users"
                "SELECT name, (SELECT count(*) FROM orders) AS order_count, \"id\" AS \"clutch__rid_0\" FROM users")
               ("ordinal order"
                "SELECT name, status FROM users ORDER BY 1"
                "SELECT name, status, \"id\" AS \"clutch__rid_0\" FROM users ORDER BY 1")))
      (pcase-let ((`(,label ,sql ,expected) case))
        (ert-info ((format "case: %s" label))
          (let ((prep (clutch--prepare-row-identity-query 'fake-conn sql)))
            (should (plist-get prep :augmented))
            (should (equal (plist-get prep :hidden-aliases)
                           '("clutch__rid_0")))
            (should (equal (plist-get prep :sql) expected))))))))

(ert-deftest clutch-test-row-identity-prep-reads-through-ctes ()
  "Row identity should be injected where a CTE chain reads its table."
  (cl-letf ((clutch--row-identity-cache (make-hash-table :test 'eq))
            (clutch--row-identity-cte-alias-suffix "5e55")
            ((symbol-function 'clutch-db-row-identity-candidates)
             (lambda (_conn _table)
               (list (list :kind 'primary-key :name "PRIMARY"
                           :columns '("id")))))
            ((symbol-function 'clutch-db-escape-identifier)
             (lambda (_conn id) (format "\"%s\"" id))))
    (dolist (case
             '(("WITH a AS (SELECT * FROM app.users WHERE active = 1), b AS (SELECT * FROM a) SELECT * FROM b"
                "WITH a AS (SELECT users.*, \"id\" AS \"clutch__rid_0_5e55\" FROM app.users WHERE active = 1), b AS (SELECT * FROM a) SELECT * FROM b"
                "app.users" star)
               ("WITH c (k, v) AS (SELECT id, name FROM users), d (x) AS (SELECT v FROM c) SELECT x FROM d"
                "WITH c (k, v, \"clutch__rid_0_5e55\") AS (SELECT id, name, \"id\" AS \"clutch__rid_0_5e55\" FROM users), d (x, \"clutch__rid_0_5e55\") AS (SELECT v, \"clutch__rid_0_5e55\" AS \"clutch__rid_0_5e55\" FROM c) SELECT x, \"clutch__rid_0_5e55\" AS \"clutch__rid_0_5e55\" FROM d"
                "users" ("name"))
               ("WITH c AS (SELECT id AS k, name AS v, upper(name) AS u FROM users) SELECT v, k, u FROM c x"
                "WITH c AS (SELECT id AS k, name AS v, upper(name) AS u, \"id\" AS \"clutch__rid_0_5e55\" FROM users) SELECT v, k, u, \"clutch__rid_0_5e55\" AS \"clutch__rid_0_5e55\" FROM c x"
                "users" ("name" "id" nil))
               ("WITH c AS (SELECT name FROM users) SELECT * FROM c"
                "WITH c AS (SELECT name, \"id\" AS \"clutch__rid_0_5e55\" FROM users) SELECT * FROM c"
                "users" ("name"))
               ("WITH users AS (SELECT * FROM audit) SELECT * FROM users"
                "WITH users AS (SELECT audit.*, \"id\" AS \"clutch__rid_0_5e55\" FROM audit) SELECT * FROM users"
                "audit" star)
               ("WITH c AS (SELECT id, name FROM users) SELECT c.name FROM c"
                "WITH c AS (SELECT id, name, \"id\" AS \"clutch__rid_0_5e55\" FROM users) SELECT c.name, \"clutch__rid_0_5e55\" AS \"clutch__rid_0_5e55\" FROM c"
                "users" ("name"))
               ("WITH ids AS (SELECT id FROM users) SELECT * FROM users WHERE id IN (SELECT id FROM ids)"
                "WITH ids AS (SELECT id FROM users) SELECT users.*, \"id\" AS \"clutch__rid_0_5e55\" FROM users WHERE id IN (SELECT id FROM ids)"
                "users" star)))
      (pcase-let ((`(,sql ,expected ,token ,projection) case))
        (ert-info (sql)
          (let ((prep (clutch--prepare-row-identity-query 'fake-conn sql)))
            (should (plist-get prep :cte))
            (should (equal (plist-get prep :sql) expected))
            (should (equal (plist-get prep :source-token) token))
            (should (equal (plist-get prep :hidden-aliases)
                           '("clutch__rid_0_5e55")))
            (should (equal (plist-get prep :writable-projection)
                           projection))))))))

(ert-deftest clutch-test-row-identity-prep-refuses-ctes-it-cannot-follow ()
  "A CTE query that does not lead to one table should not be looked up."
  (cl-letf ((clutch--row-identity-cache (make-hash-table :test 'eq))
            ((symbol-function 'clutch-db-row-identity-candidates)
             (lambda (_conn table)
               (ert-fail (format "Looked up row identity for %s" table)))))
    (dolist (sql '("WITH c AS (SELECT team, count(*) AS n FROM users GROUP BY team) SELECT * FROM c"
                   "WITH c AS (SELECT u.id FROM users u JOIN orders o ON o.user_id = u.id) SELECT * FROM c"
                   "WITH c AS (SELECT id FROM a UNION ALL SELECT id FROM b) SELECT * FROM c"
                   "WITH RECURSIVE r (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r) SELECT * FROM r"
                   "WITH \"Users\" AS (SELECT * FROM audit) SELECT * FROM users"
                   "WITH c AS (SELECT DISTINCT team FROM users) SELECT * FROM c"
                   "WITH c AS (SELECT * FROM users) SELECT team, count(*) FROM c GROUP BY team"
                   "WITH b AS (SELECT * FROM shadow), shadow AS (SELECT * FROM users) SELECT * FROM b"
                   ;; An identity column would change what the other reads see.
                   "WITH c AS (SELECT name FROM users) SELECT name FROM c WHERE name IN (SELECT * FROM c)"))
      (ert-info (sql)
        (let ((prep (clutch--prepare-row-identity-query 'fake-conn sql)))
          (should-not (plist-get prep :table))
          (should-not (plist-get prep :cte))
          (should (equal (plist-get prep :sql) sql)))))))

(ert-deftest clutch-test-row-identity-prep-skips-queries-of-table-history ()
  "A query of its table as of another time should get no row identity.
Its rows may be past versions, while an edit by key changes the current one."
  (cl-letf ((clutch--row-identity-cache (make-hash-table :test 'eq))
            ((symbol-function 'clutch-db-row-identity-candidates)
             (lambda (_conn table)
               (ert-fail (format "Looked up row identity for %s" table)))))
    (dolist (sql '("SELECT * FROM users FOR SYSTEM_TIME ALL"
                   "SELECT * FROM users FOR /* history */ SYSTEM_TIME ALL WHERE name = 'OLD'"
                   "SELECT id, name AS \"customer's name\" FROM users FOR SYSTEM_TIME ALL WHERE name = 'OLD'"
                   "SELECT * FROM users FOR VALID_TIME AS OF DATE '2020-01-01' WHERE id = 1"
                   "WITH c AS (SELECT * FROM users FOR SYSTEM_TIME ALL) SELECT * FROM c"
                   "SETTING DEFAULT VALID_TIME TO ALL SELECT * FROM users"))
      (ert-info (sql)
        (let ((prep (clutch--prepare-row-identity-query 'fake-conn sql)))
          (should (equal (plist-get prep :table) "users"))
          (should (eq (plist-get prep :identity-status) 'unsupported))
          (should (equal (plist-get prep :sql) sql)))))))

(ert-deftest clutch-test-row-identity-prep-uses-backend-source-table-name ()
  "Row identity preparation should canonicalize source tables through the backend."
  (let ((clutch--row-identity-cache (make-hash-table :test 'eq))
        requested-table)
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

(ert-deftest clutch-test-row-identity-prep-multiline-sql-mode-syntax ()
  "Multi-line SELECT * must qualify the star under sql-mode syntax.
sql-mode gives newlines comment-end syntax, so syntax-dependent whitespace
classes once let the source-table token keep a trailing newline and the
injected head became \"SELECT nil.*\" (MySQL error 1051)."
  (cl-letf ((clutch--row-identity-cache (make-hash-table :test 'eq))
            ((symbol-function 'clutch-db--source-table-name)
             (lambda (_conn token) (clutch-db-sql-table-name token)))
            ((symbol-function 'clutch-db--source-table-schema)
             (lambda (_conn token) (clutch-db-sql-table-schema token)))
            ((symbol-function 'clutch-db--source-table-catalog)
             (lambda (_conn _token) nil))
            ((symbol-function 'clutch-db-row-identity-candidates)
             (lambda (_conn _table &optional _schema _catalog)
               (list (list :kind 'primary-key
                           :name "PRIMARY"
                           :columns '("order_consign_id")))))
            ((symbol-function 'clutch-db-escape-identifier)
             (lambda (_conn id) (format "`%s`" id))))
    (with-temp-buffer
      (sql-mode)
      (let* ((sql (concat "SELECT *\n"
                          "FROM `zj`.`ffp_order_consign`\n"
                          "WHERE oil_extraction_id = 14\n"
                          "  AND NOT order_consign_id IN"
                          " (SELECT order_consign_id"
                          " FROM ffp_order_payoil_plan_relation);"))
             (prep (clutch--prepare-row-identity-query 'fake-conn sql)))
        (should (plist-get prep :augmented))
        (should-not (string-match-p "\\bnil\\b" (plist-get prep :sql)))
        (should (string-prefix-p "SELECT `ffp_order_consign`.*,"
                                 (plist-get prep :sql)))))))

(ert-deftest clutch-test-from-body-parts-newline-under-sql-mode-syntax ()
  "FROM-body token parsing must not depend on the buffer syntax table."
  (with-temp-buffer
    (sql-mode)
    (should (equal (clutch-db-sql-from-body-parts
                    " `zj`.`ffp_order_consign`\n")
                   '("`zj`.`ffp_order_consign`" nil)))
    (should (equal (clutch-db-sql-source-table
                    "SELECT *\nFROM\n  `zj`.`ffp_order_consign`\nWHERE x = 1")
                   "ffp_order_consign"))))

(ert-deftest clutch-test-row-identity-prep-skips-unqualifiable-star ()
  "A bare * whose qualifier cannot be derived must not be augmented."
  (cl-letf ((clutch--row-identity-cache (make-hash-table :test 'eq))
            ((symbol-function 'clutch-db-row-identity-candidates)
             (lambda (_conn _table)
               (list (list :kind 'primary-key
                           :name "PRIMARY"
                           :columns '("id")))))
            ((symbol-function 'clutch-db-escape-identifier)
             (lambda (_conn id) (format "\"%s\"" id)))
            ((symbol-function 'clutch-db-sql-table-qualifier)
             (lambda (_table) nil)))
    (let ((prep (clutch--prepare-row-identity-query
                 'fake-conn "SELECT * FROM users")))
      (should-not (plist-get prep :augmented))
      (should (equal (plist-get prep :sql) "SELECT * FROM users"))
      (should-not (string-match-p "\\bnil\\b" (plist-get prep :sql))))))

(ert-deftest clutch-test-row-identity-resolution-is-traced ()
  "Row identity resolution runs before execution, so the trace must show it.
Without an event its wait is invisible and looks like query time."
  (let ((clutch-debug-mode t)
        (conn (make-clutch-db-sqlite-conn :database "/tmp/debug.db")))
    (clutch--clear-debug-capture)
    (cl-letf (((symbol-function 'clutch-db-row-identity-candidates)
               (lambda (_conn _table &optional _schema _catalog)
                 (list (list :kind 'primary-key
                             :name "PRIMARY"
                             :columns '("ID"))))))
      (clutch--prepare-row-identity-query conn "SELECT name FROM users")
      (let ((debug-text (clutch-test--debug-buffer-string)))
        (should (string-match-p "row-identity" debug-text))
        (should (string-match-p "Resolved PRIMARY" debug-text))
        (should (string-match-p "Elapsed" debug-text))
        (should (string-match-p "users" debug-text))))))

(ert-deftest clutch-test-row-identity-is-cached-per-relation ()
  "Row identity is stable for a relation, so resolve it once per relation.
It runs synchronously before every execution, so repeating it charges the
user for the same metadata round trips on every query."
  (let ((clutch--row-identity-cache (make-hash-table :test 'eq))
        (conn (make-clutch-db-sqlite-conn :database "/tmp/cache.db"))
        (calls 0))
    (cl-letf (((symbol-function 'clutch-db-row-identity-candidates)
               (lambda (_conn _table &optional _schema _catalog)
                 (cl-incf calls)
                 (list (list :kind 'primary-key
                             :name "PRIMARY"
                             :columns '("id")))))
              ((symbol-function 'clutch-db-escape-identifier)
               (lambda (_conn id) (format "\"%s\"" id))))
      (let ((first (clutch--prepare-row-identity-query
                    conn "SELECT name FROM users WHERE active = 1")))
        (should (= calls 1))
        ;; A different statement against the same relation reuses it.
        (let ((second (clutch--prepare-row-identity-query
                       conn "SELECT id FROM users")))
          (should (= calls 1))
          (should (equal (plist-get first :candidate)
                         (plist-get second :candidate))))
        ;; A different relation resolves on its own.
        (clutch--prepare-row-identity-query conn "SELECT * FROM orders")
        (should (= calls 2))
        ;; Invalidation must drop it.
        (clutch--clear-connection-metadata-caches conn)
        (clutch--prepare-row-identity-query conn "SELECT id FROM users")
        (should (= calls 3))))))

(ert-deftest clutch-test-row-identity-cache-keeps-namespaces-apart ()
  "A cached identity belongs to one relation, not to a bare table name."
  (let ((clutch--row-identity-cache (make-hash-table :test 'eq))
        (conn (make-clutch-db-sqlite-conn :database "/tmp/cache.db"))
        requested)
    (cl-letf (((symbol-function 'clutch-db--source-table-schema)
               (lambda (_conn token)
                 (and (string-match "\\`\\(.+\\)\\.[^.]+\\'" token)
                      (match-string 1 token))))
              ((symbol-function 'clutch-db-row-identity-candidates)
               (lambda (_conn table &optional schema _catalog)
                 (push (cons schema table) requested)
                 (list (list :kind 'primary-key
                             :name (format "PK_%s" (or schema "default"))
                             :columns '("id")))))
              ((symbol-function 'clutch-db-escape-identifier)
               (lambda (_conn id) (format "\"%s\"" id))))
      (clutch--prepare-row-identity-query conn "SELECT id FROM app.users")
      (clutch--prepare-row-identity-query conn "SELECT id FROM ops.users")
      ;; Same bare table name, different namespaces: both must be resolved.
      (should (equal (nreverse requested)
                     '(("app" . "users") ("ops" . "users")))))))

(ert-deftest clutch-test-row-identity-failure-is-not-cached ()
  "A metadata failure must not become a permanently cached answer."
  (let ((clutch--row-identity-cache (make-hash-table :test 'eq))
        (conn (make-clutch-db-sqlite-conn :database "/tmp/cache.db"))
        (calls 0))
    (cl-letf (((symbol-function 'clutch-db-row-identity-candidates)
               (lambda (_conn _table &optional _schema _catalog)
                 (cl-incf calls)
                 (signal 'clutch-db-error '("metadata failed")))))
      (clutch--prepare-row-identity-query conn "SELECT name FROM users")
      (clutch--prepare-row-identity-query conn "SELECT name FROM users")
      (should (= calls 2)))))

(ert-deftest clutch-test-row-identity-is-forgotten-after-a-statement-without-results ()
  "DDL, USE or SET can change what a table name resolves to.
Any statement that returns no result set drops the cached row identities."
  (skip-unless (sqlite-available-p))
  (let* ((clutch--row-identity-cache (make-hash-table :test 'eq))
         (db-file (make-temp-file "clutch-row-identity-" nil ".db"))
         (conn (clutch-db-sqlite-connect (list :database db-file))))
    (unwind-protect
        (cl-flet ((identity-columns ()
                    (plist-get (plist-get (clutch--prepare-row-identity-query
                                           conn "SELECT * FROM t")
                                          :candidate)
                               :columns)))
          (clutch-db-query conn "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
          (should (equal (identity-columns) '("id")))
          (dolist (sql '("DROP TABLE t"
                         "CREATE TABLE t (code TEXT PRIMARY KEY, name TEXT)"))
            (should-not
             (plist-get (clutch-test--await-outcome
                         (lambda (k)
                           (clutch--execute-statement-attempt
                            sql conn t nil nil k)))
                        :error)))
          (should (equal (identity-columns) '("code"))))
      (clutch-db-disconnect conn)
      (ignore-errors (delete-file db-file)))))

(ert-deftest clutch-test-row-identity-is-forgotten-with-its-table-metadata ()
  "Refreshing one table's metadata also drops that table's row identity."
  (let ((clutch--table-metadata-cache (make-hash-table :test 'eq))
        (clutch--row-identity-cache (make-hash-table :test 'eq))
        (conn (make-clutch-db-sqlite-conn :database "/tmp/cache.db"))
        (calls 0))
    (cl-letf (((symbol-function 'clutch-db-row-identity-candidates)
               (lambda (&rest _)
                 (cl-incf calls)
                 (list (list :kind 'primary-key :name "PRIMARY" :columns '("id")))))
              ((symbol-function 'clutch-db-escape-identifier)
               (lambda (_conn id) (format "\"%s\"" id))))
      (clutch--prepare-row-identity-query conn "SELECT * FROM users")
      (clutch--prepare-row-identity-query conn "SELECT * FROM orders")
      (clutch--clear-table-metadata-caches conn "users")
      (clutch--prepare-row-identity-query conn "SELECT * FROM users")
      (clutch--prepare-row-identity-query conn "SELECT * FROM orders")
      (should (= calls 3)))))

(ert-deftest clutch-test-table-metadata-keys-keep-namespaces-apart ()
  "A qualified table should be cached apart from its bare name.
Clearing a table by name drops both, and no other table."
  (clutch-test--with-isolated-metadata-caches
    (let ((conn 'fake-conn)
          (qualified (clutch--table-key "people" "aux")))
      (should (equal (clutch--table-key "people") "people"))
      (should (equal (clutch--table-key-arguments "people") '("people")))
      (should (equal (clutch--table-key-arguments qualified) '("people" "aux" nil)))
      (should (equal (clutch--table-key-label qualified) "aux.people"))
      (dolist (key (list "people" qualified "orders"))
        (clutch--set-table-metadata conn key :column-details (list key)))
      (clutch--set-table-metadata conn (cons "public" "people") :comment "c")
      (clutch--set-column-details-queue conn (list qualified "orders"))
      (should (equal (clutch--cached-column-details conn qualified)
                     (list qualified)))
      (clutch--clear-table-metadata-caches conn "people")
      (should-not (clutch--column-details-cached-p conn "people"))
      (should-not (clutch--column-details-cached-p conn qualified))
      (should-not (clutch--table-metadata conn (cons "public" "people")))
      (should (clutch--column-details-cached-p conn "orders"))
      (should (equal (clutch--column-details-queue conn) '("orders"))))))

(ert-deftest clutch-test-row-identity-cache-key-runs-no-query ()
  "Keying the cache must not query the connection.
On DuckDB the current schema is a query on the session, so a failure there
would abort the user's statement before it runs."
  (let ((clutch--table-metadata-cache (make-hash-table :test 'eq))
        (clutch--row-identity-cache (make-hash-table :test 'eq))
        (conn (make-clutch-db-sqlite-conn :database "/tmp/cache.db")))
    (cl-letf (((symbol-function 'clutch-db-current-schema)
               (lambda (_conn)
                 (signal 'clutch-db-error '("current schema query failed"))))
              ((symbol-function 'clutch-db-row-identity-candidates)
               (lambda (&rest _)
                 (list (list :kind 'primary-key :name "PRIMARY" :columns '("id")))))
              ((symbol-function 'clutch-db-escape-identifier)
               (lambda (_conn id) (format "\"%s\"" id))))
      (should (equal (plist-get (plist-get (clutch--prepare-row-identity-query
                                            conn "SELECT * FROM users")
                                           :candidate)
                                :name)
                     "PRIMARY")))))

(ert-deftest clutch-test-row-identity-trace-reports-failures ()
  "A failed row identity lookup should stay visible in the trace."
  (let ((clutch-debug-mode t)
        (conn (make-clutch-db-sqlite-conn :database "/tmp/debug.db")))
    (clutch--clear-debug-capture)
    (cl-letf (((symbol-function 'clutch-db-row-identity-candidates)
               (lambda (_conn _table &optional _schema _catalog)
                 (signal 'clutch-db-error '("metadata failed")))))
      (clutch--prepare-row-identity-query conn "SELECT name FROM users")
      (let ((debug-text (clutch-test--debug-buffer-string)))
        (should (string-match-p "row-identity" debug-text))
        (should (string-match-p "metadata failed" debug-text))))))

(ert-deftest clutch-test-sql-clause-matching-ignores-buffer-syntax ()
  "Clause keywords must match the same way from every buffer.
`sql-mode' gives newlines comment-end syntax, which hid a clause split
across lines, and `_', `#' and `@' are no word constituents there or in the
standard syntax table, which let keywords match inside identifiers."
  (dolist (mode '(fundamental-mode sql-mode))
    (with-temp-buffer
      (funcall mode)
      (ert-info ((symbol-name mode))
        (should (clutch-db-sql-find-top-level-clause
                 "SELECT * FROM t\nORDER\nBY id" "ORDER\\s-+BY"))
        (should (equal (clutch--high-risk-query-reason
                        "DELETE FROM t WHERE 1 = 1\nORDER\nBY id")
                       "WHERE is always true"))
        (should (equal (clutch-db-sql-normalize "SELECT 1;\n") "SELECT 1"))
        (should (equal (clutch-db-sql-source-table
                        "SELECT id, valid_from FROM prices")
                       "prices"))
        (should (clutch--row-identity-augmentable-sql-p
                 "SELECT * FROM t WHERE group_id = 3" "t"))
        (should-not (clutch-db-sql-has-top-level-row-limit-p
                     "SELECT credit_limit FROM accounts"))
        (should (equal (clutch--high-risk-query-reason
                        "UPDATE t SET valid#where = 1")
                       "no WHERE"))
        (should (equal (clutch--high-risk-query-reason
                        "UPDATE t SET value = @where")
                       "no WHERE"))))))

(ert-deftest clutch-test-split-statement-specs-drops-comment-only-fragments ()
  "A comment after the last semicolon is no statement of its own.
Running a region or buffer that ended in one failed on the comment with
\"Statement 2 failed\"."
  (with-temp-buffer
    (should (equal (mapcar #'car (clutch--split-statement-specs
                                  "SELECT 1; -- note"))
                   '("SELECT 1")))
    (should (equal (mapcar #'car (clutch--split-statement-specs
                                  "SELECT 1;\n/* a */\nSELECT 2; -- b\n"))
                   '("SELECT 1" "/* a */\nSELECT 2")))
    (should-not (clutch--split-statement-specs "-- only a comment\n"))))

(ert-deftest clutch-test-execute-range-runs-one-statement-without-its-comment ()
  "A range with one statement and a trailing comment runs that statement.
The whole range, comment included, used to reach paging, whose LIMIT then
started a second statement after the semicolon."
  (with-temp-buffer
    (insert "SELECT 1; -- note")
    (let (ran)
      (cl-letf (((symbol-function 'clutch--execute-and-mark)
                 (lambda (sql beg end) (setq ran (list sql beg end))))
                ((symbol-function 'clutch--execute-statements)
                 (lambda (&rest _)
                   (ert-fail "one statement ran as a batch"))))
        (clutch--execute-sql-range (point-min) (point-max) "region")
        (should (equal ran '("SELECT 1" 1 9)))))))

(ert-deftest clutch-test-execute-range-runs-executable-comments ()
  "A /*! or /*M! comment is a statement, as MySQL and MariaDB run its body.
A dump script sets its session variables in them, and dropping fragments
that hold only comments skipped those settings without a word."
  (with-temp-buffer
    (insert "-- dump header\n/*!40101 SET @x=1 */;\n/*M!100100 SET @y=2 */;\n"
            "SELECT 1; /* note */ -- note\n")
    (let (ran)
      (cl-letf (((symbol-function 'clutch--execute-statements)
                 (lambda (stmts) (setq ran (mapcar #'car stmts)))))
        (clutch--execute-sql-range (point-min) (point-max) "buffer")
        (should (equal ran '("-- dump header\n/*!40101 SET @x=1 */"
                             "/*M!100100 SET @y=2 */"
                             "SELECT 1")))))))

(ert-deftest clutch-test-row-identity-qualifies-lowercase-star ()
  "A lowercase sole * is qualified whatever `case-fold-search' says.
Oracle rejects \"select *, ROWID\", which the star check exists to avoid."
  (cl-letf (((symbol-function 'clutch-db-escape-identifier)
             (lambda (_conn id) (format "\"%s\"" id))))
    (let ((case-fold-search nil))
      (should (equal (clutch--row-identity-inject-select-list
                      'fake-conn "select * from t" '("ROWID") '("clutch__rid_0"))
                     "SELECT t.*, ROWID AS \"clutch__rid_0\" from t")))))

(ert-deftest clutch-test-row-identity-prep-skips-select-lists-with-comments ()
  "Hidden identity columns are not appended to a select list with a comment.
A trailing line comment swallowed them together with FROM, and a hint hid
a sole * that Oracle rejects next to other columns (ORA-00923)."
  (cl-letf (((symbol-function 'clutch-db-row-identity-candidates)
             (lambda (&rest _)
               (list (list :kind 'primary-key :name "PRIMARY" :columns '("id")))))
            ((symbol-function 'clutch-db-escape-identifier)
             (lambda (_conn id) (format "\"%s\"" id))))
    ;; A fresh connection object per statement: identity must be resolved,
    ;; not reused from another statement or test.
    (dolist (sql '("SELECT /*+ FIRST_ROWS(10) */ * FROM emp"
                   "SELECT * -- every column\nFROM emp"
                   "SELECT id, name /* shown */ FROM emp"))
      (let ((prep (clutch--prepare-row-identity-query (list 'conn) sql)))
        (should (plist-get prep :candidate))
        (should-not (plist-get prep :augmented))
        (should (equal (plist-get prep :sql) sql))))
    (should (plist-get (clutch--prepare-row-identity-query
                        (list 'conn)
                        "SELECT id, name FROM emp WHERE id > 0 -- shown")
                       :augmented))))

(ert-deftest clutch-test-row-identity-prep-records-metadata-errors ()
  "Row identity preparation should keep metadata errors visible."
  (cl-letf (((symbol-function 'clutch-db-row-identity-candidates)
             (lambda (_conn _table)
               (signal 'clutch-db-error '("metadata failed"))))
            (clutch--row-identity-cache (make-hash-table :test 'eq)))
    (let ((prep (clutch--prepare-row-identity-query
                 'fake-conn "SELECT name FROM users")))
      (should (equal (plist-get prep :identity-status) 'error))
      (should (equal (plist-get prep :identity-error-message)
                     "metadata failed"))
      (should-not (plist-get prep :candidate))
      (should (equal (plist-get prep :sql) "SELECT name FROM users")))))

(ert-deftest clutch-test-row-identity-prep-select-star-qualifies-star ()
  "SELECT * row identity injection should qualify the star before adding columns."
  (cl-letf ((clutch--row-identity-cache (make-hash-table :test 'eq))
            ((symbol-function 'clutch-db-row-identity-candidates)
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

(ert-deftest clutch-test-row-identity-prep-skips-non-row-preserving-selects ()
  "Aggregate, ambiguous, joined, or derived SELECTs should not be augmented."
  (dolist (case
           '((primary-key fake-conn
              (:kind primary-key :name "PRIMARY" :columns ("id"))
              ("SELECT count(1) FROM users"
               "SELECT COUNT(*) AS n FROM users"
               "SELECT count(*) FILTER (WHERE active) FROM users"
               "SELECT max(id) FROM users WHERE active = 1"
               "SELECT coalesce(sum(amount), 0) AS total FROM orders"
               "SELECT listagg(name, ',') WITHIN GROUP (ORDER BY name) FROM users"
               "SELECT u.name, o.total FROM users u JOIN orders o ON o.user_id = u.id"
               "SELECT * FROM users, orders"
               "WITH x AS (SELECT count(*) AS n FROM users) SELECT * FROM x"
               "SELECT * FROM (SELECT * FROM users) u"))
             (row-locator oracle-conn
              (:kind row-locator :name "ROWID"
               :select-expressions ("ROWID") :where-sql "ROWID = ?")
              ("SELECT COUNT(*) FROM users"))))
    (pcase-let ((`(,label ,conn ,candidate ,sqls) case))
      (ert-info ((format "candidate: %s" label))
        (cl-letf ((clutch--row-identity-cache (make-hash-table :test 'eq))
                  ((symbol-function 'clutch-db-row-identity-candidates)
                   (lambda (_conn _table) (list candidate)))
                  ((symbol-function 'clutch-db-escape-identifier)
                   (lambda (_conn id) (format "\"%s\"" id))))
          (dolist (sql sqls)
            (let ((prep (clutch--prepare-row-identity-query conn sql)))
              (should-not (plist-get prep :augmented))
              (should (equal (plist-get prep :sql) sql)))))))))

(ert-deftest clutch-test-row-identity-prep-skips-oracle-dictionary-metadata ()
  "Oracle dictionary views should skip all row identity metadata probes."
  (let ((conn (make-clutch-jdbc-conn :params '(:driver oracle
                                               :schema "ZJSY"))))
    (cl-letf (((symbol-function 'clutch-db-primary-key-columns)
               (lambda (&rest _args)
                 (ert-fail "Dictionary views must skip primary-key metadata")))
              ((symbol-function 'clutch-jdbc--unique-not-null-identities)
               (lambda (&rest _args)
                 (ert-fail "Dictionary views must skip unique-key metadata")))
              ((symbol-function 'clutch-db-search-table-entries)
               (lambda (_conn prefix)
                 (list (list :name prefix :type "PUBLIC SYNONYM"
                             :schema "SYS" :source-schema "PUBLIC")))))
      (dolist (sql '("SELECT table_name FROM all_tables"
                     "SELECT table_name FROM user_tables"))
        (let ((prep (clutch--prepare-row-identity-query conn sql)))
          (should (equal (plist-get prep :identity-status) 'unsupported))
          (should-not (plist-get prep :candidate))
          (should-not (plist-get prep :augmented))
          (should (equal (plist-get prep :sql) sql))
          (should-not (string-match-p "\\bROWID\\b"
                                      (plist-get prep :sql))))))))

(ert-deftest clutch-test-row-identity-prep-scopes-qualified-oracle-source ()
  "Qualified Oracle sources should resolve identity in their named schema."
  (let ((conn (make-clutch-jdbc-conn :params '(:driver oracle
                                               :schema "ZJSY"))))
    (cl-letf (((symbol-function 'clutch-db-primary-key-columns)
               (lambda (metadata-conn table)
                 (should (equal table "REPORTS"))
                 (should (equal
                          (plist-get (clutch-jdbc-conn-params metadata-conn)
                                     :schema)
                          "APP"))
                 nil))
              ((symbol-function 'clutch-jdbc--unique-not-null-identities)
               (lambda (metadata-conn table)
                 (should (equal table "REPORTS"))
                 (should (equal
                          (plist-get (clutch-jdbc-conn-params metadata-conn)
                                     :schema)
                          "APP"))
                 nil))
              ((symbol-function 'clutch-db-search-table-entries)
               (lambda (metadata-conn prefix)
                 (should (equal prefix "REPORTS"))
                 (if (equal (plist-get (clutch-jdbc-conn-params metadata-conn)
                                       :schema)
                            "APP")
                     '((:name "REPORTS" :type "VIEW"
                        :schema "APP" :source-schema "APP"))
                   '((:name "REPORTS" :type "TABLE"
                      :schema "ZJSY" :source-schema "ZJSY"))))))
      (let ((prep (clutch--prepare-row-identity-query
                   conn "SELECT id FROM APP.reports")))
        (should (equal (plist-get prep :table) "REPORTS"))
        (should (equal (plist-get prep :source-token) "APP.reports"))
        (should (eq (plist-get prep :identity-status) 'unsupported))
        (should-not (plist-get prep :augmented))
        (should (equal (plist-get prep :sql)
                       "SELECT id FROM APP.reports"))))))

(ert-deftest clutch-test-qualified-update-preserves-target-and-default-syntax ()
  "Qualified UPDATE targets and DEFAULT assignments should remain SQL syntax."
  (let ((clutch-connection
         (make-clutch-jdbc-conn :params '(:driver oracle)))
        (clutch--result-columns '("ID" "STATUS" "NOTE"))
        (clutch--result-column-defs
         '((:name "ID" :backend-type "NUMBER" :source-column "ID")
           (:name "STATUS" :backend-type "VARCHAR2"
            :source-column "STATUS")
           (:name "NOTE" :backend-type "VARCHAR2" :source-column "NOTE")))
        (identity '(:kind primary-key :name "PRIMARY"
                    :table "REPORTS" :source-token "APP.reports"
                    :columns ("ID") :indices (0) :source-indices (0))))
    (cl-letf (((symbol-function 'clutch--ensure-column-details)
               (lambda (_conn _table &optional _strict)
                 '((:name "ID" :backend-type "NUMBER")
                   (:name "STATUS" :backend-type "VARCHAR2" :default "'new'")
                   (:name "NOTE" :backend-type "VARCHAR2")))))
      (pcase-let ((`(,update-sql . ,update-params)
                   (clutch-result--build-update-stmt
                    "REPORTS" [7]
                    (list (cons 1 clutch--cell-default-placeholder)
                          (cons 2 "ready"))
                    identity
                    (clutch-result--update-source-columns
                     "REPORTS" '(1 2) "test")))
                  (`(,delete-sql . ,_)
                   (clutch-result--build-delete-stmt-for-identity
                    "REPORTS" [7] identity)))
        (should (equal update-sql
                       (concat "UPDATE APP.reports SET \"STATUS\" = DEFAULT, "
                               "\"NOTE\" = ? WHERE \"ID\" = ?")))
        (should (equal (clutch-db-param-values update-params) '("ready" 7)))
        (should (string-prefix-p "DELETE FROM APP.reports WHERE" delete-sql))))))

(ert-deftest clutch-test-jdbc-update-uses-schema-type-for-blob-parameter ()
  "JDBC staged updates should retain BLOB type from column metadata."
  (let ((clutch-connection
         (make-clutch-jdbc-conn :params '(:driver oracle)))
        (clutch--result-columns '("CONTENT" "STATUS"))
        (clutch--result-column-defs
         '((:name "CONTENT" :type-category blob :source-column "CONTENT")
           (:name "STATUS" :type-category numeric :source-column "STATUS")))
        (identity '(:kind row-locator :name "ROWID"
                    :table "DOCUMENTS" :where-sql "ROWID = ?"
                    :indices (2))))
    (cl-letf (((symbol-function 'clutch--ensure-column-details)
               (lambda (_conn _table &optional _strict)
                 (clutch-jdbc--normalize-column-details
                  '((:name "CONTENT" :type "BLOB")
                    (:name "STATUS" :type "NUMBER"))))))
      (pcase-let ((`(,_sql . ,params)
                   (clutch-result--build-update-stmt
                    "DOCUMENTS" ["AAAPr9AAEAAAACXAAA"]
                    '((0 . "{\"message\":\"中文\"}")
                      (1 . "1"))
                    identity
                    (clutch-result--update-source-columns
                     "DOCUMENTS" '(0 1) "test"))))
        (should (equal (mapcar #'clutch-db-param-type params)
                       '("BLOB" "NUMBER" nil)))))))

(ert-deftest clutch-test-jdbc-blob-edit-retains-source-encoding ()
  "Editing a JDBC text BLOB should retain its source byte encoding."
  (let (captured)
    (with-temp-buffer
      (insert "{\"message\":\"已修改\"}")
      (setq-local clutch-result-edit--special-value nil
                  clutch-result-edit--initial-value-state
                  '(nil . "{\"message\":\"原值\"}")
                  clutch-result-edit--blob-encoding "GB18030"
                  clutch-result-edit--column-name "CONTENT"
                  clutch-result-edit--column-def '(:type-category blob)
                  clutch-result-edit--column-detail '(:type "BLOB")
                  clutch-result--edit-callback
                  (lambda (value) (setq captured value)))
      (cl-letf (((symbol-function 'clutch-result-edit--refresh-record-return-buffer)
                 #'ignore)
                ((symbol-function 'clutch-result-edit--clear-active-target)
                 #'ignore)
                ((symbol-function 'clutch-result-edit--restore-result-position)
                 #'ignore)
                ((symbol-function 'quit-window) #'ignore))
        (clutch-result-edit--finish-buffer)))
    (should (equal captured "{\"message\":\"已修改\"}"))
    (should (equal (get-text-property
                    0 'clutch-jdbc-blob-encoding captured)
                   "GB18030"))))

(ert-deftest clutch-test-update-canonicalizes-source-column-case ()
  "Mutation SQL should quote the backend's canonical column spelling."
  (let ((clutch-connection
         (make-clutch-jdbc-conn :params '(:driver oracle)))
        (clutch--result-columns '("name"))
        (clutch--result-column-defs
         '((:name "name" :source-column "name")))
        (identity '(:kind primary-key :name "PRIMARY"
                    :table "USERS" :columns ("ID") :indices (1))))
    (cl-letf (((symbol-function 'clutch--ensure-column-details)
               (lambda (_conn _table &optional _strict)
                 '((:name "NAME" :backend-type "VARCHAR2")))))
      (pcase-let ((`(,sql . ,_)
                   (clutch-result--build-update-stmt
                    "USERS" [7] '((0 . "Ada")) identity
                    (clutch-result--update-source-columns "USERS" '(0) "test"))))
        (should (string-search "SET \"NAME\" = ?" sql))))))

(ert-deftest clutch-test-update-uses-canonical-source-behind-alias ()
  "Mutation SQL should not quote a display alias or raw identifier casing."
  (let ((clutch-connection
         (make-clutch-jdbc-conn :params '(:driver jdbc)))
        (clutch--result-columns '("display_name"))
        (clutch--result-column-defs
         '((:name "display_name" :source-column "NAME")))
        (identity '(:kind primary-key :name "PRIMARY"
                    :table "users" :columns ("id") :indices (1))))
    (cl-letf (((symbol-function 'clutch--ensure-column-details)
               (lambda (_conn _table &optional _strict)
                 '((:name "name" :backend-type "text")))))
      (pcase-let ((`(,sql . ,_)
                   (clutch-result--build-update-stmt
                    "users" [7] '((0 . "Ada")) identity
                    (clutch-result--update-source-columns "users" '(0) "test"))))
        (should (string-search "SET \"name\" = ?" sql))
        (should-not (string-search "display_name" sql))
        (should-not (string-search "\"NAME\"" sql))))))

(ert-deftest clutch-test-source-column-metadata-match-is-safe ()
  "Canonical source lookup should prefer exact names and reject ambiguity."
  (let ((clutch-connection 'fake-conn)
        (clutch--result-columns '("display"))
        (clutch--result-column-defs '((:source-column "Foo"))))
    (should
     (equal (plist-get
             (clutch-result--writable-source-detail
              "items" 0 "test" '((:name "foo") (:name "Foo")))
             :name)
            "Foo"))
    (setq clutch--result-column-defs '((:source-column "FOO")))
    (should-error
     (clutch-result--writable-source-detail
      "items" 0 "test" '((:name "foo") (:name "Foo")))
     :type 'user-error)
    (setq clutch--result-column-defs '((:source-column "name")))
    (should
     (equal (plist-get
             (clutch-result--writable-source-detail
              "items" 0 "test" '((:name "NAME")))
             :name)
            "NAME"))))

(ert-deftest clutch-test-row-identity-finalize-separates-hidden-and-source-pk ()
  "Hidden locator indices and visible source PK indices should stay distinct."
  (let* ((prep (list :table "users"
                     :source-token "APP.users"
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
    (should (equal (plist-get row-identity :source-indices) '(0)))
    (should (equal (plist-get row-identity :source-token) "APP.users"))))

(ert-deftest clutch-test-row-identity-uses-verified-trailing-injected-column ()
  "A user projection matching the hidden alias must not become row identity."
  (let* ((prep (list :table "users"
                     :candidate (list :kind 'primary-key
                                      :name "PRIMARY"
                                      :columns '("id"))
                     :hidden-aliases '("clutch__rid_0")
                     :writable-projection '("manager_id" "id")
                     :augmented t))
         (columns (clutch--apply-row-identity-column-metadata
                   '((:name "clutch__rid_0")
                     (:name "id")
                     (:name "clutch__rid_0"))
                   prep))
         (row-identity (clutch--finalize-row-identity prep columns)))
    (should-not (plist-get (nth 0 columns) :hidden))
    (should (plist-get (nth 2 columns) :hidden))
    (should (equal (plist-get row-identity :indices) '(2)))
    (should (equal (clutch-db-row-identity-values
                    '(99 7 42) row-identity)
                   [42]))))

(ert-deftest clutch-test-writable-projection-requires-direct-source-columns ()
  "Computed and uncertain projections must stay read-only."
  (should (eq (clutch--writable-select-projection "SELECT * FROM products")
              'star))
  (should (eq (clutch--writable-select-projection "SELECT p.* FROM products p")
              'star))
  (should (equal (clutch--writable-select-projection
                  "SELECT p.price, id FROM products p")
                 '("price" "id")))
  (should (equal (clutch--writable-select-projection
                  "SELECT price AS retail_price FROM products")
                 '("price")))
  (should (equal (clutch--writable-select-projection
                  "SELECT price * 1.2 AS price, id FROM products")
                 '(nil "id")))
  (should (equal (clutch--writable-select-projection
                  "SELECT price retail_price FROM products")
                 '(nil)))
  (let* ((prep '(:hidden-aliases ("clutch__rid_0")
                 :writable-projection (nil "id")))
         (defs (clutch--apply-row-identity-column-metadata
                '((:name "price") (:name "id") (:name "clutch__rid_0"))
                prep))
         (clutch--base-query "SELECT price * 1.2 AS price, id FROM products")
         (clutch--result-columns '("price" "id" "clutch__rid_0"))
         (clutch--result-column-defs defs))
    (should (plist-member (nth 0 defs) :source-column))
    (should-not (plist-get (nth 0 defs) :source-column))
    (should (equal (plist-get (nth 1 defs) :source-column) "id"))
    (should-error (clutch-result--writable-source-column 0 "edit cell")
                  :type 'user-error)
    (should (equal (clutch-result--writable-source-column 1 "edit cell")
                   "id"))))

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

(ert-deftest clutch-test-horizontal-paging-moves-point-into-view ()
  "`]' and `[' should leave point in the first column of the new view.
Point left behind made the next cell command scroll back to it."
  (let ((buf (generate-new-buffer " *clutch-paging*"))
        (clutch-column-padding 1))
    (unwind-protect
        (save-window-excursion
          (set-window-buffer (selected-window) buf)
          (with-current-buffer buf
            (clutch-test--init-result-state
             (list :columns (mapcar (lambda (i) (format "column_%02d" i))
                                    (number-sequence 1 20))
                   :rows (list (mapcar (lambda (i) (format "value_%02d" i))
                                       (number-sequence 1 20)))
                   :column-widths (make-vector 20 9)
                   :render t))
            (cl-flet ((point-column ()
                        (get-text-property (point) 'clutch-col-idx))
                      (first-in-view-p ()
                        (let ((cidx (get-text-property (point) 'clutch-col-idx)))
                          (and (>= (clutch--column-border-position cidx)
                                   (window-hscroll))
                               (or (zerop cidx)
                                   (< (clutch--column-border-position (1- cidx))
                                      (window-hscroll)))))))
              (goto-char (aref clutch--row-start-positions 0))
              (clutch-result-first-column)
              (clutch-result-scroll-right)
              (let ((hscroll (window-hscroll))
                    (cidx (point-column)))
                (should (> hscroll 0))
                (should (first-in-view-p))
                (clutch-result-next-cell)
                (should (= (point-column) (1+ cidx)))
                (should (= (window-hscroll) hscroll))
                (clutch-result-scroll-left)
                (should (< (window-hscroll) hscroll))
                (should (first-in-view-p))))))
      (kill-buffer buf))))

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

(ert-deftest clutch-test-window-size-changes-coalesce-redraws ()
  "Repeated resize notifications should schedule one result redraw."
  (let ((schedule-count 0)
        (refresh-count 0))
    (with-temp-buffer
      (let ((buffer (current-buffer)))
        (setq-local major-mode 'clutch-result-mode
                    clutch--column-widths [10]
                    clutch--last-window-width 80)
        (cl-letf (((symbol-function 'window-list)
                   (lambda (&rest _args) '(fake-window)))
                  ((symbol-function 'window-buffer)
                   (lambda (_window) buffer))
                  ((symbol-function 'window-body-width)
                   (lambda (_window &optional _pixelwise) 100))
                  ((symbol-function 'timerp)
                   (lambda (timer)
                     (and (consp timer) (eq (car timer) 'fake-timer))))
                  ((symbol-function 'run-at-time)
                   (lambda (_delay _repeat fn &rest _args)
                     (when (eq fn #'clutch--run-column-width-refresh)
                       (cl-incf schedule-count))
                     '(fake-timer)))
                  ((symbol-function 'clutch--refresh-display)
                   (lambda () (cl-incf refresh-count))))
          (dotimes (_ 20)
            (clutch--window-size-change nil))
          (should (= schedule-count 1))
          (should (= refresh-count 0))
          (should (timerp clutch--column-width-refresh-timer)))))))

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

(ert-deftest clutch-test-result-cursor-ui-does-not-leak-mode-line-position ()
  "Result cursor updates should not alter another buffer's position display."
  (let ((original (default-value 'mode-line-position))
        (other (generate-new-buffer " *clutch-mode-line-position-test*")))
    (unwind-protect
        (with-temp-buffer
          (clutch-test--setup-rendered-result)
          (clutch--goto-cell 1 2)
          (setq clutch--last-cell-position nil)
          (let ((this-command 'clutch-result-next-cell))
            (run-hook-wrapped
             'post-command-hook
             (lambda (function)
               (when (eq function #'clutch--sync-result-cursor-ui)
                 (funcall function))
               nil)))
          (should (equal clutch--last-cell-position '(1 . 2)))
          (should (equal (default-value 'mode-line-position) original))
          (with-current-buffer other
            (should (equal mode-line-position original))))
      (setq-default mode-line-position original)
      (when (buffer-live-p other)
        (kill-buffer other)))))

(ert-deftest clutch-test-scroll-command-clamps-point-below-table ()
  "Scrolling past the table should leave point on the last rendered row."
  (save-window-excursion
    (with-temp-buffer
      (switch-to-buffer (current-buffer))
      (clutch-test--setup-rendered-result)
      (clutch--goto-cell 1 2)
      (set-window-hscroll (selected-window) 12)
      (goto-char (point-max))
      (let ((this-command 'mwheel-scroll))
        (run-hooks 'post-command-hook))
      (should (= (get-text-property (point) 'clutch-row-idx) 2))
      (should (= (get-text-property (point) 'clutch-col-idx) 2))
      (should (= (window-hscroll) 12)))))

(ert-deftest clutch-test-goto-cell-uses-row-starts-and-fallbacks ()
  "Cell navigation should use cached row starts and fall back within a row."
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
      (should (= (point) (+ row1 2)))))
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

(ert-deftest clutch-test-down-cell-stays-in-last-result-cell ()
  "Repeated row navigation should stop in the last result cell."
  (dolist (case '((0 ((1 "alpha" "oslo")))
                  (2 ((1 "alpha" "oslo")
                      (2 "bravo" "rome")))
                  (1 ((1 "alpha" "oslo")
                      (2 "bravo" "rome")
                      (3 "charlie" "paris")
                      (4 "delta" "lima")
                      (5 "echo" "tokyo")))))
    (pcase-let ((`(,cidx ,rows) case))
      (ert-info ((format "%d rows, column %d" (length rows) cidx))
        (with-temp-buffer
          (clutch-test--setup-rendered-result rows)
          (clutch--goto-cell 0 cidx)
          (dotimes (ridx (1- (length rows)))
            (clutch-result-down-cell)
            (should (= (get-text-property (point) 'clutch-row-idx)
                       (1+ ridx)))
            (should (= (get-text-property (point) 'clutch-col-idx) cidx)))
          (let ((last-point (point)))
            (dotimes (_ 2)
              (clutch-result-down-cell)
              (should (= (point) last-point)))))))))

;;;; Rendering — row and separator rendering

(ert-deftest clutch-test-refresh-display-preserves-visible-row-position ()
  "Refreshing the result view should not drift point downward on screen."
  (save-window-excursion
    (let ((buf (get-buffer-create " *clutch-refresh-display*")))
      (unwind-protect
          (progn
            (switch-to-buffer buf)
            (with-current-buffer buf
              (clutch-test--init-result-state
               (list :columns '("c1" "c2" "c3" "c4" "c5" "c6")
                     :column-defs '(nil nil nil nil nil nil)
                     :rows (cl-loop for i from 1 to 40
                                    collect
                                    (cl-loop for suffix in '("a" "b" "c"
                                                             "d" "e" "f")
                                             collect
                                             (format "row%02d-%s" i suffix)))
                     :page-total-rows 40
                     :column-widths [16 16 16 16 16 16]))
              (clutch--refresh-display)
              (let* ((win (selected-window))
                     (top-ridx 10)
                     (point-ridx 15))
                (set-window-start win (aref clutch--row-start-positions top-ridx))
                (set-window-hscroll win 24)
                (goto-char (aref clutch--row-start-positions point-ridx))
                (forward-char 2)
                (let ((before-top-ridx
                       (save-excursion
                         (goto-char (window-start win))
                         (clutch--row-idx-at-line)))
                      (before-hscroll (window-hscroll win))
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
                  (should (= (window-hscroll win) before-hscroll))
                  (clutch--refresh-display)
                  (should (= (save-excursion
                               (goto-char (window-start win))
                               (clutch--row-idx-at-line))
                             before-top-ridx))
                  (should (= (count-screen-lines (window-start win)
                                                 (line-beginning-position))
                             before-line))
                  (should (= (window-hscroll win) before-hscroll))))))
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
            (clutch-test--init-result-state
             (list :columns '("name")
                   :column-defs '(nil)
                   :rows '(("aa"))
                   :page-total-rows 1
                   :column-widths [4]))
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
                       (lambda (name _cidx)
                         name))
                      ((symbol-function 'clutch--refresh-footer-line) #'ignore))
              (clutch--refresh-display)
              (should (equal clutch--column-pixel-widths [80])))))
      (when (window-live-p result-win)
        (delete-window result-win))
      (when (buffer-live-p buf)
        (kill-buffer buf)))))

(ert-deftest clutch-test-replace-row-at-index-contract ()
  "Row replacement should update safely or fall back to a full refresh."
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
        (should (string-match-p "^│D" after1)))))
  (with-temp-buffer
    (clutch-test--setup-rendered-result)
    (let ((old-third-start (aref clutch--row-start-positions 2)))
      (setq-local clutch--pending-edits
                  (list (cons (cons (vector 2) 2) "表表表")))
      (clutch--goto-cell 1 2)
      (clutch--replace-row-at-index 1)
      (should (= (get-text-property (point) 'clutch-row-idx) 1))
      (should (= (get-text-property (point) 'clutch-col-idx) 2))
      (let ((actual-third-start (save-excursion
                                  (goto-char (point-min))
                                  (forward-line 2)
                                  (point))))
        (should (= (aref clutch--row-start-positions 2) actual-third-start))
        (should (/= old-third-start actual-third-start)))))
  (with-temp-buffer
    (let (refreshed)
      (setq-local clutch--result-rows '((1 "alpha" "oslo"))
                  clutch--filtered-rows nil
                  clutch--column-widths [3 8 8])
      (cl-letf (((symbol-function 'clutch--refresh-display)
                 (lambda ()
                   (setq refreshed t))))
        (clutch--replace-row-at-index 0)
        (should refreshed))))
  (with-temp-buffer
    (clutch-result-mode)
    (setq-local clutch--result-columns '("name")
                clutch--result-column-defs '(nil)
                clutch--result-rows '(("aa"))
                clutch--filtered-rows nil
                clutch--pending-edits nil
                clutch--pending-deletes nil
                clutch--pending-inserts nil
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
               (lambda (name _cidx)
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

(ert-deftest clutch-test-append-and-delete-pending-insert-row-contract ()
  "Pending insert row append/delete should update rendered text and row starts."
  (with-temp-buffer
    (clutch-test--setup-rendered-result)
    (setq-local clutch-connection 'fake-conn
                clutch--result-source-table "users"
                clutch--pending-inserts
                '((("name" . "dana") ("city" . "lima"))))
    (cl-letf (((symbol-function 'clutch--cached-column-details)
               (lambda (_conn _table)
                 '((:name "id")
                   (:name "name")
                   (:name "city"))))
              ((symbol-function 'clutch--ensure-column-details-async)
               (lambda (&rest _)
                 (error "cached details should avoid async placeholder load"))))
      (clutch--append-pending-insert-row 0)
      (should (= (length clutch--row-start-positions) 4))
      (let ((line (substring-no-properties (clutch-test--rendered-line-at 3))))
        (should (string-prefix-p "│I I1 " line))
        (should (string-match-p "dana" line))
        (should (string-match-p "lima" line)))
      (clutch--delete-row-at-index 3)
      (should (= (length clutch--row-start-positions) 3))
      (should-not (string-match-p "dana" (buffer-string))))))

(ert-deftest clutch-test-filter-empty-state-follows-staged-inserts ()
  "The empty filter hint must follow zero-to-one ghost-row transitions."
  (clutch-test--with-result-state
   (:connection nil :filter-pattern "missing" :render t)
   (should (string-match-p "No matches on this page" (buffer-string)))
   (setq-local clutch--pending-inserts '((("name" . "new"))))
   (clutch--append-pending-insert-row 0)
   (should-not (string-match-p "No matches" (buffer-string)))
   (should (= 1 (length clutch--row-start-positions)))
   (should (= (point-min) (aref clutch--row-start-positions 0)))
   (setq-local clutch--pending-inserts nil)
   (clutch--delete-row-at-index 0)
   (should (= 0 (length clutch--row-start-positions)))
   (should (string-match-p "No matches on this page" (buffer-string)))))

(ert-deftest clutch-test-delete-pending-insert-middle-row-falls-back ()
  "Deleting a non-final rendered row should fall back to a full redraw."
  (with-temp-buffer
    (clutch-test--setup-rendered-result)
    (setq-local clutch--pending-inserts
                '((("name" . "dana")) (("name" . "erin"))))
    (let (refreshed)
      (cl-letf (((symbol-function 'clutch--refresh-display)
                 (lambda ()
                   (setq refreshed t))))
        (clutch--delete-row-at-index 3)
        (should refreshed)))))

(ert-deftest clutch-test-render-row-displays-null-placeholder ()
  "Result cells should display database NULL as a compact placeholder."
  (with-temp-buffer
    (setq-local clutch--result-column-defs '((:name "name" :type-category text)))
    (let ((cell (clutch--render-row '(nil) 0 '(0) [8] nil)))
      (should (string-match-p (regexp-quote "<null>") cell))
      (should (text-property-any 0 (length cell) 'face 'clutch-null-face cell))
      (should (equal (get-text-property 3 'clutch-full-value cell) nil)))))

(ert-deftest clutch-test-render-row-highlights-active-edit-target ()
  "Cells open in an edit buffer should use the staged-edit highlight."
  (with-temp-buffer
    (setq-local clutch--result-column-defs '((:name "id" :type-category numeric)
                                             (:name "name" :type-category text)))
    (let ((cell (clutch--render-row
                 '(1 "before") 0 '(1) [4 8]
                 (list :active-edit-cell (cons 0 1)))))
      (should (text-property-any 0 (length cell)
                                 'face 'clutch-modified-face cell)))))

(ert-deftest clutch-test-display-select-contract ()
  :tags '(:smoke)
  "SELECT display should install source metadata, errors, and window metrics."
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
           'fake-conn sql result 0
           :row-identity-prep prep
           :server-pageable t
           :result-context context
           :source-buffer (current-buffer))
          (with-current-buffer result-name
            (should (equal clutch--result-source-table "orders")))))))
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
                   (lambda (name _cidx)
                     name))
                  ((symbol-function 'clutch--refresh-footer-line) #'ignore))
          (with-temp-buffer
            (select-window source-win)
            (clutch-result--display-select
             'fake-conn "SELECT name FROM users" result 0
             :server-pageable t
             :source-buffer (current-buffer)))
          (with-current-buffer result-name
            (should (equal clutch--column-pixel-widths [100]))))
      (when (window-live-p result-win)
        (delete-window result-win))
      (when-let* ((buf (get-buffer result-name)))
        (kill-buffer buf)))))

(ert-deftest clutch-test-execute-select-scopes-row-identity-problem-details ()
  "A successful result should retain only its identity metadata diagnostics."
  (let* ((result-name "*clutch-row-identity-problem*")
         (debug-name " *clutch-row-identity-debug*")
         (other (generate-new-buffer " *clutch-other-problem-owner*"))
         (clutch-debug-mode nil)
         (clutch-debug-buffer-name debug-name)
         (clutch--problem-records-by-conn (make-hash-table :test 'eq))
         (conn (make-clutch-jdbc-conn :conn-id 7
                                      :params '(:driver oracle)))
         (result (make-clutch-db-result
                  :connection conn
                  :columns '((:name "id" :type-category numeric))
                  :rows '((1))))
         (details '(:backend oracle
                    :summary "ORA-12592: TNS:bad packet"
                    :diag (:category "metadata"
                           :op "get-columns"
                           :sql-state "66000"
                           :vendor-code 12592
                           :context (:table "USERS"))))
         (identity-error
          (list 'clutch-db-error "ORA-12592: TNS:bad packet" details)))
    (unwind-protect
        (clutch-test--with-result-buffer (result-name)
          (cl-letf (((symbol-function 'clutch-db-build-paged-sql)
                     (lambda (_conn sql _page-num _page-size
                                    &optional _order-by _page-offset)
                       sql))
                    ((symbol-function 'clutch-db-row-identity-candidates)
                     (lambda (&rest _args)
                       (signal (car identity-error) (cdr identity-error))))
                    ((symbol-function 'clutch-db-query-async) #'ignore)
                    ((symbol-function 'clutch-db-query)
                     (lambda (_conn _sql) result)))
            (clutch-test--execute-and-present
             "SELECT * FROM users" conn))
          (with-current-buffer result-name
            (let ((diag (plist-get clutch--buffer-error-details :diag)))
              (should (equal (plist-get diag :op) "get-columns"))
              (should (equal (plist-get diag :sql-state) "66000"))
              (should (= (plist-get diag :vendor-code) 12592))
              (should (equal (plist-get (plist-get diag :context) :table)
                             "USERS"))))
          (let ((clutch-debug-mode t))
            (clutch--clear-debug-capture)
            (clutch--replay-problem-records-to-debug-buffer))
          (should (string-match-p "Operation: get-columns"
                                  (clutch-test--debug-buffer-string)))
          (let ((other-problem '(:summary "other buffer failure")))
            (clutch--remember-problem-record
             :buffer other :connection conn :problem other-problem)
            (clutch-result--display-select
             conn "SELECT * FROM users" result 0
             :row-identity-prep
             '(:sql "SELECT * FROM users"
               :table "users"
               :identity-status unsupported)
             :server-pageable t
             :source-buffer (current-buffer))
            (with-current-buffer result-name
              (should-not clutch--buffer-error-details))
            (let ((entry (gethash conn clutch--problem-records-by-conn)))
              (should (eq (plist-get entry :buffer) other))
              (should (equal (plist-get entry :problem) other-problem)))
            (with-current-buffer other
              (should (equal clutch--buffer-error-details other-problem)))
            (clutch--forget-problem-record nil conn)
            (should-not (gethash conn clutch--problem-records-by-conn))))
      (when-let* ((buffer (get-buffer debug-name)))
        (kill-buffer buffer))
      (when (buffer-live-p other)
        (kill-buffer other)))))

(ert-deftest clutch-test-display-select-clears-stale-result-state ()
  "A fresh SELECT should not keep the previous result's state in its buffer."
  (let ((result-name "*clutch-stale-result*")
        (result (make-clutch-db-result
                 :columns '((:name "name" :type-category text))
                 :rows '(("alice")))))
    (clutch-test--with-result-buffer (result-name)
      (clutch-result--display-select
       'fake-conn "SELECT name FROM orders" result 0
       :server-pageable t
       :result-context '(:server-rewritable t :source-table "orders"))
      (with-current-buffer result-name
        (should (equal clutch--result-source-table "orders"))
        (should clutch--result-server-pageable)
        (should clutch--result-server-rewritable)
        (setq-local clutch--dml-result t
                    clutch--where-filter "id > 10"
                    header-line-format "stale"
                    mode-line-format "stale"))
      (clutch-result--display-select
       'fake-conn "SELECT name FROM users" result 0)
      (with-current-buffer result-name
        (should-not clutch--result-source-table)
        (should-not clutch--result-server-pageable)
        (should-not clutch--result-server-rewritable)
        (should-not clutch--dml-result)
        (should-not clutch--where-filter)
        (should-not header-line-format)
        (should-not (local-variable-p 'mode-line-format))))))

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
                clutch--result-column-defs
                '((:name "id" :type-category numeric :source-column "id")
                  (:name "name" :type-category text :source-column "name"))
                clutch--result-rows '((1 "before"))
                clutch--filtered-rows nil
                clutch-result-max-rows 100
                clutch--page-current 0
                clutch--column-widths [4 8]
                clutch--row-identity (clutch-test--primary-row-identity
                                      "users" '("id") '(0))
                clutch--pending-edits '((([1] . 1) . "edited"))
                clutch--pending-deletes nil)
    (let ((row-positions (make-vector 1 nil)))
      (clutch--insert-data-rows '((1 "before"))
                                row-positions
                                (clutch--visible-columns)
                                (clutch--effective-widths)
                                (clutch--row-number-digits)
                                (clutch--build-render-state))
      (should (string-prefix-p "│E  1 " (buffer-string))))))

(ert-deftest clutch-test-record-render-contract ()
  "Record render should reflect result state, display rules, and context."
  (clutch-test--with-result-state-buffer result-buf
      (:columns '("id" "name")
       :column-defs '((:name "id" :type-category numeric)
                      (:name "name" :type-category text))
       :rows '((1 "before"))
       :row-identity (clutch-test--primary-row-identity "users" '("id") '(0))
       :pending-edits '((([1] . 1) . "edited")))
    (with-temp-buffer
      (setq-local clutch-record--result-buffer result-buf
                  clutch-record--row-idx 0
                  clutch-record--expanded-fields nil)
      (clutch-record--render)
      (let ((rendered (buffer-string)))
        (should (string-match-p "edited" rendered))
        (should-not (string-match-p "before" rendered)))))
  (clutch-test--with-result-state-buffer result-buf
      (:columns '("id" "note")
       :column-defs '((:name "id" :type-category numeric)
                      (:name "note" :type-category text))
       :rows '((1 nil)))
    (with-temp-buffer
      (setq-local clutch-record--result-buffer result-buf
                  clutch-record--row-idx 0
                  clutch-record--expanded-fields nil)
      (clutch-record--render)
      (let ((rendered (buffer-string))
            (case-fold-search nil))
        (should (string-match-p (regexp-quote clutch--null-cell-display-text)
                                rendered))
        (should-not (string-match-p "\\`Field\\s-*:" rendered))
        (should-not (string-match-p " : NULL\\b" rendered))
        (should (text-property-any (point-min) (point-max)
                                   'face 'clutch-null-face)))))
  (clutch-test--with-result-state-buffer result-buf
      (:connection 'fake-conn
       :connection-params '(:backend mysql :host "db")
       :columns '("id" "name")
       :column-defs '((:name "id" :type-category numeric)
                      (:name "name" :type-category text))
       :rows '((1 "before")))
    (with-current-buffer result-buf
      (setq-local clutch--conn-sql-product 'mysql))
    (with-temp-buffer
      (clutch-record-mode)
      (setq-local clutch-record--result-buffer result-buf
                  clutch-record--row-idx 0
                  clutch-record--expanded-fields nil)
      (clutch-record--render)
      (should (eq clutch-connection 'fake-conn))
      (should (equal clutch--connection-params '(:backend mysql :host "db")))
      (should (eq clutch--conn-sql-product 'mysql))))
  (let ((result-buf (generate-new-buffer "*clutch-result*")))
    (kill-buffer result-buf)
    (with-temp-buffer
      (clutch-record-mode)
      (setq-local clutch-record--result-buffer result-buf
                  clutch-record--row-idx 0
                  clutch-record--expanded-fields nil)
      (should-error (clutch-record--render) :type 'user-error))))

(ert-deftest clutch-test-record-field-line-edits-through-result-buffer ()
  "Record fields should edit through their parent result buffer."
  (clutch-test--with-pop-to-buffer-capture edit-buf
    (clutch-test--with-result-state-buffer result-buf
        (:connection nil
         :connection-params '(:backend mysql)
         :source-table "users"
         :columns '("id" "name")
         :column-defs '((:name "id" :type-category numeric)
                        (:name "name" :type-category text))
         :rows '((1 "alice"))
         :row-identity (clutch-test--primary-row-identity
                        "users" '("id") '(0)))
      (with-temp-buffer
        (let ((record-buf (current-buffer)))
          (with-current-buffer record-buf
            (clutch-record-mode)
            (should (eq revert-buffer-function #'clutch-record--render))
            (setq-local clutch-record--result-buffer result-buf
                        clutch-record--row-idx 0
                        clutch-record--expanded-fields nil)
            (clutch-record--render)
            (goto-char (point-min))
            (search-forward "name")
            (goto-char (match-beginning 0))
            (should (equal (clutch--cell-at-point) '(0 1 "alice"))))
          (cl-letf (((symbol-function 'clutch--ensure-column-details)
                     (lambda (_conn _table &optional _strict)
                       (list (list :name "name" :type "text")))))
            (with-current-buffer record-buf
              (should (eq (clutch-result-edit-cell) edit-buf))))
          (with-current-buffer edit-buf
            (should (eq clutch-result--edit-result-buffer result-buf))
            (should (eq clutch-result-edit--return-buffer record-buf))
            (erase-buffer)
            (insert "ann")
            (cl-letf (((symbol-function 'clutch--replace-row-at-index) #'ignore)
                      ((symbol-function 'clutch--refresh-footer-line) #'ignore)
                      ((symbol-function 'quit-window) #'ignore)
                      ((symbol-function 'message) #'ignore))
              (clutch-result-edit-finish)))
          (with-current-buffer record-buf
            (should (string-match-p "name\\s-*:\\s-*ann" (buffer-string)))
            (should (eq (get-text-property (point) 'clutch-col-idx) 1))))))))

(ert-deftest clutch-test-record-edit-unchanged-numeric-does-not-stage ()
  "Submitting an unchanged numeric Record field should not stage an edit."
  (clutch-test--with-pop-to-buffer-capture edit-buf
    (clutch-test--with-result-state-buffer result-buf
        (:connection nil
         :connection-params '(:backend mysql)
         :source-table "orders"
         :columns '("id" "qty")
         :column-defs '((:name "id" :type-category numeric)
                        (:name "qty" :type-category numeric))
         :rows '((1 42))
         :row-identity (clutch-test--primary-row-identity
                        "orders" '("id") '(0)))
      (with-temp-buffer
        (let ((record-buf (current-buffer)))
          (with-current-buffer record-buf
            (clutch-record-mode)
            (setq-local clutch-record--result-buffer result-buf
                        clutch-record--row-idx 0
                        clutch-record--expanded-fields nil)
            (clutch-record--render)
            (goto-char (point-min))
            (search-forward "qty")
            (goto-char (match-beginning 0)))
          (cl-letf (((symbol-function 'clutch--ensure-column-details)
                     (lambda (_conn _table &optional _strict)
                       (list (list :name "qty" :type "int")))))
            (with-current-buffer record-buf
              (clutch-result-edit-cell)))
          (with-current-buffer edit-buf
            (should (equal (buffer-string) "42"))
            (cl-letf (((symbol-function 'clutch--replace-row-at-index) #'ignore)
                      ((symbol-function 'clutch--refresh-footer-line) #'ignore)
                      ((symbol-function 'quit-window) #'ignore)
                      ((symbol-function 'message) #'ignore))
              (clutch-result-edit-finish)))
          (with-current-buffer result-buf
            (should-not clutch--pending-edits)))))))

(ert-deftest clutch-test-record-discard-pending-edit-at-field ()
  "Record buffers should discard the staged edit for the field at point."
  (clutch-test--with-result-state-buffer result-buf
      (:columns '("id" "name")
       :column-defs '((:name "id" :type-category numeric)
                      (:name "name" :type-category text))
       :rows '((1 "alice"))
       :row-identity (clutch-test--primary-row-identity
                      "users" '("id") '(0))
       :pending-edits '((([1] . 1) . "ann")))
    (with-temp-buffer
      (clutch-record-mode)
      (setq-local clutch-record--result-buffer result-buf
                  clutch-record--row-idx 0
                  clutch-record--expanded-fields nil)
      (clutch-record--render)
      (should (string-match-p "name\\s-*:\\s-*ann" (buffer-string)))
      (goto-char (point-min))
      (search-forward "name")
      (goto-char (match-beginning 0))
      (cl-letf (((symbol-function 'clutch--replace-row-at-index) #'ignore)
                ((symbol-function 'clutch--refresh-footer-line) #'ignore)
                ((symbol-function 'message) #'ignore))
        (clutch-result-discard-pending-at-point))
      (should (string-match-p "name\\s-*:\\s-*alice" (buffer-string)))
      (should (eq (get-text-property (point) 'clutch-col-idx) 1)))
    (with-current-buffer result-buf
      (should-not clutch--pending-edits))))

(ert-deftest clutch-test-record-open-renders-visible-row ()
  "Opening record view should render the visible row at point."
  (dolist (case '((unfiltered nil nil 1 nil)
                  (filtered "bob" ((2 "bob")) 0 "alice")))
    (pcase-let ((`(,label ,filter ,filtered-rows ,row-prop ,rejected) case))
      (ert-info ((format "case: %s" label))
        (clutch-test--with-pop-to-buffer-capture record-buf
          (clutch-test--with-result-state
              (:connection nil
               :connection-params nil
               :columns '("id" "name")
               :column-defs '((:name "id" :type-category numeric)
                              (:name "name" :type-category text))
               :rows '((1 "alice") (2 "bob") (3 "carol"))
               :filter-pattern filter
               :filtered-rows filtered-rows
               :column-widths [2 5]
               :render t)
            (setq-local clutch--conn-sql-product nil)
            (goto-char (point-min))
            (let ((match (text-property-search-forward
                          'clutch-row-idx row-prop #'eq)))
              (should match)
              (goto-char (prop-match-beginning match)))
            (clutch-result-open-record)
            (should (buffer-live-p record-buf))
            (with-current-buffer record-buf
              (let ((rendered (buffer-string)))
                (should (string-match-p "id" rendered))
                (should (string-match-p "name" rendered))
                (should (string-match-p "id\\s-*:\\s-*2" rendered))
                (should (string-match-p "name\\s-*:\\s-*bob" rendered))
                (when rejected
                  (should-not (string-match-p rejected rendered)))))))))))

(ert-deftest clutch-test-record-row-navigation ()
  "Record view should move by visible rows and stop at boundaries."
  (dolist (case '((clutch-record-next-row 0
                   ((1 "alice") (2 "bob") (3 "carol")) nil 1 nil)
                  (clutch-record-next-row 2
                   ((1 "alice") (2 "bob") (3 "carol")) nil nil
                   "Already at last row")
                  (clutch-record-next-row 0
                   ((1 "alice") (2 "bob")) ((2 "bob")) nil
                   "Already at last row")
                  (clutch-record-prev-row 2
                   ((1 "alice") (2 "bob") (3 "carol")) nil 1 nil)
                  (clutch-record-prev-row 0
                   ((1 "alice") (2 "bob") (3 "carol")) nil nil
                   "Already at first row")))
    (pcase-let ((`(,command ,start ,rows ,filtered ,expected ,message) case))
      (clutch-test--with-result-state-buffer result-buf
          (:rows rows
           :filter-pattern (and filtered "filtered")
           :filtered-rows filtered)
        (with-temp-buffer
          (clutch-record-mode)
          (setq-local clutch-record--result-buffer result-buf
                      clutch-record--row-idx start
                      clutch-record--expanded-fields '(0))
          (let ((renders 0))
            (cl-letf (((symbol-function 'clutch-record--render)
                       (lambda () (cl-incf renders))))
              (if message
                  (let ((err (should-error (funcall command)
                                           :type 'user-error)))
                    (should (string-match-p message
                                            (error-message-string err)))
                    (should (= renders 0)))
                (funcall command)
                (should (= clutch-record--row-idx expected))
                (should-not clutch-record--expanded-fields)
                (should (= renders 1))))))))))

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
            (put-text-property (point-min) (point-max) 'clutch-full-value
                               clutch--cell-default-placeholder)
            (should (equal (clutch-record--field-action-description) "Show value"))
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

;;;; Rendering — header-line and footer

(ert-deftest clutch-test-header-line-display-contract ()
  "Header-line display should track hscroll and preserve pixel alignment."
  (ert-info ("hscroll offset")
    (with-temp-buffer
      (setq-local clutch--header-line-string "0123456789")
      (cl-letf (((symbol-function 'window-hscroll)
                 (lambda (&optional _window) 3)))
        (should (equal (clutch--header-line-with-hscroll) "3456789")))))
  (ert-info ("display-space crop")
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
  (ert-info ("sort indicator glyph crop preserves following alignment")
    (with-temp-buffer
      (setq-local clutch--sort-column nil)
      (let ((clutch--header-sort-indicator-cache (make-hash-table :test 'equal))
            (wide-icon (propertize "I" 'display '(raise 0.0)))
            cropped)
        (cl-letf (((symbol-function 'display-graphic-p)
                   (lambda (&optional _display) t))
                  ((symbol-function 'default-font-width)
                   (lambda () 10))
                  ((symbol-function 'window-hscroll)
                   (lambda (&optional _window) 2))
                  ((symbol-function 'string-pixel-width)
                   #'clutch-test--fake-pixel-width)
                  ((symbol-function 'clutch--icon)
                   (lambda (&rest _args) wide-icon)))
          (let ((indicator (clutch--header-sort-indicator "score" 0)))
            (setq-local clutch--header-line-string (concat indicator "x")
                        clutch--column-pixel-widths [30])
            (setq cropped (clutch--header-line-with-hscroll))
            (should (equal (get-text-property 0 'display cropped)
                           '(space :width (5))))
            (should-not (get-display-property 0 'min-width cropped))
            (should (= (next-single-property-change
                        0 'display cropped (length cropped))
                       1)))))))
  (ert-info ("display prefix align-to")
    (with-temp-buffer
      (setq-local clutch--header-line-string "abc")
      (cl-letf (((symbol-function 'window-hscroll)
                 (lambda (&optional _window) 0)))
        (let ((rendered (clutch--header-line-display)))
          (should (equal (substring rendered 1) "abc"))
          (should (equal (get-text-property 0 'display rendered)
                         '(space :align-to 0))))))))

(ert-deftest clutch-test-header-line-crop-computed-once-per-offset ()
  "Redisplay evaluates the header on every frame.  At offset zero the rendered
header is returned as is, and a crop is computed once per offset, font width,
header string and column pixel widths, then reused."
  (with-temp-buffer
    (let ((header (copy-sequence "0123456789"))
          (hscroll 0)
          (font-width 10)
          (crops 0))
      (setq-local clutch--header-line-string header
                  clutch--column-pixel-widths [30])
      (cl-letf (((symbol-function 'display-graphic-p)
                 (lambda (&optional _display) t))
                ((symbol-function 'default-font-width)
                 (lambda () font-width))
                ((symbol-function 'window-hscroll)
                 (lambda (&optional _window) hscroll))
                ((symbol-function 'clutch--pixel-crop-left)
                 (lambda (string pixels)
                   (cl-incf crops)
                   (substring string (/ pixels 10)))))
        (should (eq (clutch--header-line-with-hscroll) header))
        (should (= crops 0))
        (setq hscroll 3)
        (should (equal (clutch--header-line-with-hscroll) "3456789"))
        (should (equal (clutch--header-line-with-hscroll) "3456789"))
        (should (= crops 1))
        (setq hscroll 4)
        (should (equal (clutch--header-line-with-hscroll) "456789"))
        (should (= crops 2))
        (setq font-width 20)
        (clutch--header-line-with-hscroll)
        (should (= crops 3))
        (setq-local clutch--header-line-string (copy-sequence "0123456789"))
        (clutch--header-line-with-hscroll)
        (should (= crops 4))
        (setq-local clutch--column-pixel-widths [40])
        (clutch--header-line-with-hscroll)
        (should (= crops 5))))))

(ert-deftest clutch-test-active-header-face-covers-cell-width ()
  "The active header face should cover the full cell, including padding."
  (with-temp-buffer
    (setq-local clutch--result-columns '("id")
                clutch--result-column-defs '((:name "id"))
                clutch--column-pixel-widths nil)
    (let* ((clutch--header-sort-indicator-cache
            (make-hash-table :test 'equal))
           (cell (clutch--header-cell 0 [8] 0)))
      (should
       (cl-loop for pos from 1 below (length cell)
                for face = (get-text-property pos 'face cell)
                always (or (eq face 'clutch-header-active-face)
                           (and (listp face)
                                (memq 'clutch-header-active-face face)))))
      (should-not (eq (get-text-property 0 'face cell)
                      'clutch-header-active-face)))))

(ert-deftest clutch-test-refresh-chrome-lines-update-without-changing-body ()
  "Header and footer refreshes should update chrome without touching body text."
  (ert-info ("footer")
    (with-temp-buffer
      (clutch-test--setup-rendered-result)
      (let ((body (buffer-string))
            (before (substring-no-properties clutch--footer-base-string)))
        (setq-local clutch--page-total-rows 9)
        (clutch--refresh-footer-line)
        (should (equal body (buffer-string)))
        (should-not (equal before
                           (substring-no-properties clutch--footer-base-string)))
        (should (string-match-p
                 "9" (substring-no-properties clutch--footer-base-string))))))
  (ert-info ("header")
    (with-temp-buffer
      (clutch-test--setup-rendered-result)
      (let ((body (buffer-string))
            (before (substring-no-properties clutch--header-line-string)))
        (setq-local clutch--sort-column "name"
                    clutch--sort-descending t)
        (clutch--refresh-header-line)
        (should (equal body (buffer-string)))
        (should-not (equal before
                           (substring-no-properties
                            clutch--header-line-string)))))))

(ert-deftest clutch-test-footer-filter-parts-contract ()
  "Footer filter parts should omit SQL preview and include aggregate summaries."
  (with-temp-buffer
    (setq-local clutch--last-query "SELECT id FROM t")
    (should (equal (clutch--footer-filter-parts) nil)))
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
                clutch--connection-render-state
                '(:connected-p t :transaction-state dirty)
                clutch--order-by '("created_at" . "desc")
                clutch--pending-edits '(a)
                clutch--pending-deletes '(b)
                clutch--pending-inserts '(c))
    (let ((footer (substring-no-properties
                   (clutch--render-footer 10 0 500 100))))
      (should (string-match-p "Tx: Manual\\*" footer))
      (should (string-match-p "DESC\\[created_at\\]" footer))
      (should (string-match-p "E-1 D-1 I-1" footer))
      (should (string-match-p "C-c C-c" footer))
      (should (string-match-p "C-c C-k" footer))
      (should-not (string-match-p "commit:" footer))
      (should-not (string-match-p "discard:" footer)))
    (dolist (state '(auto dirty uncertain))
      (setq-local clutch--connection-render-state
                  (list :connected-p t :transaction-state state)
                  clutch--order-by
                  (cons (make-string 100 ?x) "DESC"))
      (let ((visible (truncate-string-to-width
                      (clutch--render-footer 500 0 500 nil nil t) 60)))
        (should (string-match-p "Tx:" visible))
        (when (eq state 'uncertain)
          (should (string-match-p "Tx: Uncertain" visible)))
        (should (string-match-p "E-1 D-1 I-1" visible))))))

(ert-deftest clutch-test-render-footer-row-range-contract ()
  "Footer should show global row ranges and omit page-count segments."
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
    (should (string-match-p (regexp-quote "0 of 0 rows") empty-page)))
  (let ((footer (substring-no-properties
                 (clutch--render-footer 78 1 500 578))))
    (should-not (string-match-p "[0-9]+/[0-9]+" footer))))

(ert-deftest clutch-test-header-cell-label-distinguishes-local-duplicate-sort-columns ()
  "Local sort indicators should target the sorted duplicate column index."
  (with-temp-buffer
    (setq-local clutch--result-columns '("score" "score")
                clutch--result-column-defs '((:name "score") (:name "score"))
                clutch--sort-column "score"
                clutch--sort-descending nil
                clutch--local-sort-column-index 1)
    (let ((clutch--header-sort-indicator-cache (make-hash-table :test 'equal))
          calls)
      (cl-letf (((symbol-function 'clutch--icon)
                 (lambda (spec fallback &rest _args)
                   (push (list spec fallback) calls)
                   fallback)))
        (should (equal (substring-no-properties
                        (clutch--header-cell-label 0 10))
                       "score ↕"))
        (should (equal (substring-no-properties
                        (clutch--header-cell-label 1 10))
                       "score ↑"))
        ;; Both cells consulted the icon channel; which glyph set backs
        ;; it is cosmetic identity the fallbacks above already pin.
        (should (= (length calls) 2))))
    (let ((clutch--header-sort-indicator-cache (make-hash-table :test 'equal)))
      (cl-letf (((symbol-function 'clutch--icon)
                 (lambda (_spec _fallback &rest _args) "I")))
        (should (equal (substring-no-properties
                        (clutch--header-cell-label 1 10))
                       "score I"))))
    (let ((clutch--header-sort-indicator-cache (make-hash-table :test 'equal))
          (narrow-icon (copy-sequence " "))
          (wide-icon (copy-sequence " ")))
      (put-text-property 0 1 'display '(space :width (8)) narrow-icon)
      (put-text-property 0 1 'display '(space :width (25)) wide-icon)
      (cl-letf (((symbol-function 'display-graphic-p)
                 (lambda (&optional _display) t))
                ((symbol-function 'default-font-width)
                 (lambda () 10))
                ((symbol-function 'string-pixel-width)
                 #'clutch-test--fake-pixel-width)
                ((symbol-function 'clutch--icon)
                 (let ((icons (list narrow-icon wide-icon)))
                   (lambda (&rest _args)
                     (pop icons)))))
        (let ((narrow (clutch--header-sort-indicator "score" 1)))
          (should (= (string-width narrow) 1))
          (should (equal (get-text-property 1 'display narrow)
                         '(space :width (2))))
          (should-not (get-display-property 1 'min-width narrow)))
        (clrhash clutch--header-sort-indicator-cache)
        (let ((wide (clutch--header-sort-indicator "score" 1)))
          (should (= (string-width wide) 3))
          (should (equal (get-text-property 1 'display wide)
                         '(space :width (5))))
          (should-not (get-display-property 1 'min-width wide))
          (should-not (stringp (get-text-property 0 'display wide))))))))

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
            (let* ((cell (clutch--header-cell 1 (vector 6 8)))
                   (pos (text-property-any 0 (length cell)
                                           'clutch-header-col 1 cell))
                   (map (get-text-property pos 'local-map cell)))
              (setq command
                    (lookup-key map [header-line mouse-1]))))
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

(ert-deftest clutch-test-render-footer-warns-without-leaking-row-identity-errors ()
  "Footer should flag disabled editing without displaying backend errors."
  (dolist (case '((missing nil nil)
                  (metadata-error error "ORA-12592: TNS:bad packet")))
    (pcase-let ((`(,label ,status ,message) case))
      (ert-info ((format "case: %s" label))
        (clutch-test--with-result-state
            (:columns '("id" "name")
             :source-table "users"
             :last-query "SELECT * FROM users"
             :row-identity-status status
             :row-identity-error-message message)
          (let* ((capability (clutch--footer-mutation-capability-part))
                 (text (substring-no-properties capability))
                 (help (get-text-property 0 'help-echo capability)))
            (should (string-match-p "row editing unavailable" text))
            (should (string-match-p "E/D off" text))
            (should-not (string-prefix-p "row editing unavailable" text))
            (should-not (string-match-p "ORA-12592\\|row identity error" text))
            (should (stringp help))
            (should-not (string-match-p "ORA-12592" help))
            (when (eq status 'error)
              (should (string-match-p "clutch-debug-mode" help)))
            (should (equal (get-text-property 0 'face capability)
                           '(:inherit font-lock-warning-face
                             :weight normal)))))))))

;;;; Filter

(ert-deftest clutch-test-client-filter-preserves-page-summary ()
  "Filtering must not change page extent or infer a smaller query total."
  (dolist (spec '((5 nil nil) (5 8 nil) (3 8 5)))
    (pcase-let ((`(,size ,total ,offset) spec))
      (clutch-test--with-result-state
       (:rows '((6 "six") (7 "seven") (8 "eight"))
              :page-current 1 :page-total-rows total :result-max-rows size)
       (setq-local clutch--page-offset offset
                   clutch--connection-render-state '(:connected-p t))
       (clutch--render-result)
       (let ((summary clutch--footer-base-string))
         (dolist (pattern '("eight" "missing"))
           (cl-letf (((symbol-function 'read-string)
                      (lambda (&rest _) pattern)))
             (call-interactively (key-binding (kbd "/"))))
           (should (equal summary clutch--footer-base-string))
           (should (string-match-p
                    (if (equal pattern "eight") "1/3 page matches"
                      "0/3 page matches")
                    (clutch--footer-mode-line-display)))
           (when (equal pattern "missing")
             (should (string-match-p "No matches on this page" (buffer-string)))
             (should (string-match-p "/.*clear" (buffer-string)))
             (should-not (text-property-not-all
                          (point-min) (point-max) 'clutch-row-idx nil))))
         (cl-letf (((symbol-function 'read-string) (lambda (&rest _) "")))
           (call-interactively (key-binding (kbd "/"))))
         (should (equal summary clutch--footer-base-string))
         (should (= 3 (length (clutch--result-display-rows))))
         (should (= 3 (length clutch--row-start-positions)))
         (should-not (string-match-p "No matches" (buffer-string))))))))

(ert-deftest clutch-test-filter-apply-state ()
  "Client-side filtering should update display rows and pattern."
  (dolist (case
           '((substring
              ("id" "name")
              ((:name "id" :type-category numeric)
               (:name "name" :type-category text))
              ((1 "alice") (2 "bob") (3 "carol"))
              "ALI" ((1 "alice")) "ALI")
             (formatted-value
              ("id" "value")
              ((:name "id" :type-category numeric)
               (:name "value" :type-category numeric))
              ((1 nil) (2 42) (3 "hello"))
              "42" ((2 42)) "42")
             (no-matches
              ("id" "name")
              ((:name "id") (:name "name"))
              ((1 "alice") (2 "bob"))
              "missing" nil "missing")))
    (pcase-let ((`(,label ,columns ,column-defs ,rows
                          ,pattern ,expected-rows ,expected-pattern)
                 case))
      (ert-info ((format "case: %s" label))
        (clutch-test--with-result-state
            (:columns columns
             :column-defs column-defs
             :rows rows)
          (cl-letf (((symbol-function 'clutch--render-result) #'ignore))
            (clutch-result--apply-filter pattern)
            (should (equal (clutch--result-display-rows) expected-rows))
            (should (equal clutch--filter-pattern expected-pattern))))))))

(ert-deftest clutch-test-filter-clear-restores-all-rows ()
  "Clearing the client-side filter should restore the full result set."
  (clutch-test--with-result-state
      (:columns '("id" "name")
       :column-defs '((:name "id" :type-category numeric)
                      (:name "name" :type-category text))
       :rows '((1 "alice") (2 "bob") (3 "carol"))
       :column-widths [2 5])
    (cl-letf (((symbol-function 'clutch--render-result) #'ignore))
      (clutch-result--apply-filter "ali")
      (cl-letf (((symbol-function 'read-string) (lambda (&rest _args) "")))
        (clutch-result-filter))
      (should-not clutch--filtered-rows)
      (should-not clutch--filter-pattern))))

(ert-deftest clutch-test-apply-filter-contract ()
  "WHERE filtering should validate, rewrite, clear, and execute consistently."
  (with-temp-buffer
    (should-error (clutch-result-apply-filter) :type 'user-error))
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
                 (lambda (_current columns default-col _conn)
                   (setq seen (list columns default-col))
                   "id > 1"))
                ((symbol-function 'clutch--execute) #'ignore))
        (clutch-result-apply-filter)
        (should (equal seen '(("id" "name") "id"))))))
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn
                clutch--last-query "SELECT * FROM t"
                clutch--base-query "SELECT * FROM t"
                clutch--result-source-table "t"
                clutch--result-server-pageable t
                clutch--result-server-rewritable t
                clutch--where-filter "id > 5")
    (let (captured)
      (cl-letf (((symbol-function 'clutch--execute)
                 (lambda (sql &optional result-context)
                   (setq captured (list sql result-context)))))
        (clutch-test--with-minibuffer-answers '("")
          (clutch-result-apply-filter))
        ;; The cleared filter state is installed with the new result.
        (should (equal captured
                       '("SELECT * FROM t"
                         (:base-query nil :where-filter nil
                          :keep-result-on-error t
                          :success-message "Filter cleared")))))))
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

(ert-deftest clutch-test-where-filter-real-sqlite-workflow ()
  "\\`W' should change, mistype, write out and clear a filter like a user.
Answers follow the real readers, where an empty answer to a read with a
default returns the default."
  (skip-unless (sqlite-available-p))
  (let* ((conn (clutch-db-sqlite-connect '(:database ":memory:")))
         (source (generate-new-buffer " *clutch-where-filter-source*"))
         (clutch--execution-refresh-timer nil)
         shown
         result)
    (cl-flet ((filter (&rest answers)
                (with-current-buffer result
                  (setq shown nil)
                  (cl-letf (((symbol-function 'message)
                             (lambda (fmt &rest args)
                               (when fmt
                                 (push (apply #'format-message fmt args) shown)))))
                    (clutch-test--with-minibuffer-answers answers
                      (clutch-result-apply-filter)
                      (clutch-test--await-queries)))))
              (ids ()
                (with-current-buffer result
                  (mapcar #'car clutch--result-rows)))
              (point-on (name)
                (with-current-buffer result
                  (setq-local clutch--header-active-col
                              (cl-position name clutch--result-columns
                                           :test #'string=)))))
      (unwind-protect
          (save-window-excursion
            (clutch-db-query conn "CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)")
            (clutch-db-query conn "INSERT INTO people VALUES (1, 'ann', 10), (2, 'bob', 20), (3, 'abe', 30)")
            (set-window-buffer (selected-window) source)
            (with-current-buffer source
              (clutch-mode)
              (setq-local clutch-connection conn
                          clutch--connection-params
                          '(:backend sqlite :database ":memory:"))
              (insert "SELECT id, name, score FROM people ORDER BY id")
              (clutch-execute-buffer)
              (setq result clutch--last-result-buffer))
            (set-window-buffer (selected-window) result)
            (filter "score" "> 15")
            (should (equal (ids) '(2 3)))
            (should (member "Filter applied: WHERE \"score\" > 15" shown))
            ;; Pressing W again picks a column, matched in any case.
            (filter "SCORE" "> 25")
            (should (equal (ids) '(3)))
            ;; A filter typed wrong keeps the result and the filter it had.
            (filter "score >")
            (should (equal (ids) '(3)))
            (should (equal (buffer-local-value 'clutch--where-filter result)
                           "\"score\" > 25"))
            (should (string-suffix-p "(result unchanged)" (car shown)))
            ;; Text other than a column name is the whole condition, shown
            ;; as typed once the result arrives.
            (filter "`name` LIKE 'a%'")
            (should (equal (ids) '(1 3)))
            (should (member "Filter applied: WHERE `name` LIKE 'a%'" shown))
            ;; With point on a column, RET picks it and an empty condition
            ;; clears the filter.
            (point-on "score")
            (filter "" "")
            (should (equal (ids) '(1 2 3)))
            (should-not (buffer-local-value 'clutch--where-filter result))
            (should (member "Filter cleared" shown))
            ;; A mistake without a filter keeps the query, so g and the
            ;; next filter still start from it.
            (filter "score >")
            (should (equal (ids) '(1 2 3)))
            (with-current-buffer result
              (clutch-result-rerun)
              (clutch-test--await-queries))
            (should (equal (ids) '(1 2 3)))
            (filter "score" "> 25")
            (should (equal (ids) '(3)))
            ;; Without a column at point, RET asks for the whole condition.
            (point-on "nothing")
            (filter "" "id = 2")
            (should (equal (ids) '(2))))
        (when (buffer-live-p result)
          (kill-buffer result))
        (kill-buffer source)
        (when (clutch-db-live-p conn)
          (clutch-db-disconnect conn))))))

(ert-deftest clutch-test-where-filter-column-prompt-prefers-exact-names ()
  "A name at the column prompt should pick the column it spells exactly.
Matching in any case only picks a column when no other one matches."
  (cl-letf (((symbol-function 'clutch-db-escape-identifier)
             (lambda (_conn name) (format "\"%s\"" name))))
    (dolist (case '((("SCORE" "score") "score" "\"score\" > 15")
                    (("SCORE" "score") "SCORE" "\"SCORE\" > 15")
                    (("SCORE" "name") "score" "\"SCORE\" > 15")))
      (pcase-let ((`(,columns ,answer ,expected) case))
        (clutch-test--with-minibuffer-answers (list answer "> 15")
          (should (equal (clutch--read-where-filter nil columns nil 'conn)
                         expected)))))
    ;; Another spelling of two columns that differ only in case names
    ;; neither, so it is the whole condition.
    (clutch-test--with-minibuffer-answers '("Score")
      (should (equal (clutch--read-where-filter nil '("SCORE" "score") nil 'conn)
                     "Score")))))

(ert-deftest clutch-test-kept-result-error-still-warns-of-unknown-transaction ()
  "A filter that loses the connection should still show the error page.
Only a server error on a live connection keeps the result, so a lost
connection keeps its warning that the transaction outcome is unknown."
  (let (hint retired)
    (with-temp-buffer
      (cl-letf (((symbol-function 'clutch--connection-alive-p) (lambda (_conn) nil))
                ((symbol-function 'clutch--tx-unresolved-p) (lambda (_conn) t))
                ((symbol-function 'clutch--remember-execute-error)
                 (lambda (&rest _) (cons "Connection closed" "Connection closed")))
                ((symbol-function 'clutch-result--display-error)
                 (lambda (_conn _sql _summary _message &optional _elapsed shown)
                   (setq hint shown)
                   nil))
                ((symbol-function 'clutch--retire-query-connection)
                 (lambda (_conn) (setq retired t)))
                ((symbol-function 'message) #'ignore))
        (clutch--present-statement-outcome
         "SELECT 1" 'fake-conn
         (list :error '(clutch-db-error "Connection closed")
               :connection 'fake-conn
               :elapsed 0.1
               :result-context '(:keep-result-on-error t)
               :source-buffer (current-buffer)))))
    (should retired)
    (should (string-match-p
             (regexp-quote clutch--transaction-outcome-unknown-message)
             (or hint "")))))

(ert-deftest clutch-test-rerun-keeps-filter-real-sqlite-workflow ()
  "\\`g' and the refresh after a submit should keep a server-side filter.
They ran the filtered SQL without its context, which left the result
without its filter, \\`W' and row editing."
  (skip-unless (sqlite-available-p))
  (let* ((conn (clutch-db-sqlite-connect '(:database ":memory:")))
         (source (generate-new-buffer " *clutch-rerun-filter-source*"))
         (clutch--execution-refresh-timer nil)
         result)
    (cl-flet ((column (index)
                (with-current-buffer result
                  (mapcar (lambda (row) (nth index row)) clutch--result-rows)))
              (filtered-and-editable-p ()
                (with-current-buffer result
                  (and (equal clutch--where-filter "\"score\" > 15")
                       (clutch-result--server-rewritable-p)
                       clutch--row-identity
                       t))))
      (unwind-protect
          (save-window-excursion
            (clutch-db-query conn "CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)")
            (clutch-db-query conn "INSERT INTO people VALUES (1, 'ann', 10), (2, 'bob', 20), (3, 'abe', 30)")
            (set-window-buffer (selected-window) source)
            (with-current-buffer source
              (clutch-mode)
              (setq-local clutch-connection conn
                          clutch--connection-params
                          '(:backend sqlite :database ":memory:"))
              (insert "SELECT id, name, score FROM people ORDER BY id")
              (clutch-execute-buffer)
              (setq result clutch--last-result-buffer))
            (set-window-buffer (selected-window) result)
            (with-current-buffer result
              (clutch-test--with-minibuffer-answers '("score" "> 15")
                (clutch-result-apply-filter)
                (clutch-test--await-queries)))
            (should (equal (column 0) '(2 3)))
            (with-current-buffer result
              (clutch-result-rerun)
              (clutch-test--await-queries))
            (should (equal (column 0) '(2 3)))
            (should (filtered-and-editable-p))
            ;; Submitting an edit refreshes the result the same way.
            (with-current-buffer result
              (let ((row (car clutch--result-rows)))
                (clutch-result--apply-edit
                 0 1 "bea"
                 (list :identity (clutch-db-row-identity-values
                                  row clutch--row-identity)
                       :original (nth 1 row)
                       :original-state (cons nil (nth 1 row)))))
              (cl-letf (((symbol-function 'yes-or-no-p)
                         (lambda (&rest _) t)))
                (clutch-result-submit)
                (clutch-test--await-queries)))
            (should (equal (column 1) '("bea" "abe")))
            (should (filtered-and-editable-p))
            (with-current-buffer result
              (clutch-test--with-minibuffer-answers '("score" "> 25")
                (clutch-result-apply-filter)
                (clutch-test--await-queries)))
            (should (equal (column 0) '(3))))
        (when (buffer-live-p result)
          (kill-buffer result))
        (kill-buffer source)
        (when (clutch-db-live-p conn)
          (clutch-db-disconnect conn))))))

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

(ert-deftest clutch-test-column-displayer-custom-set-invalidates-cache ()
  "Customize updates should invalidate custom displayer render caches."
  (let ((old-displayers clutch-column-displayers)
        (old-version clutch--column-displayer-version)
        (first (lambda (_value) "first")))
    (unwind-protect
        (progn
          (setq clutch-column-displayers nil
                clutch--column-displayer-version 0)
          (funcall (get 'clutch-column-displayers 'custom-set)
                   'clutch-column-displayers
                   `(("Orders" . (("Status" . ,first)))))
          (should (= clutch--column-displayer-version 1))
          (should (eq (clutch--lookup-column-displayer "orders" "status")
                      first)))
      (setq clutch-column-displayers old-displayers
            clutch--column-displayer-version old-version))))

(ert-deftest clutch-test-cell-display-content-custom-displayer-contract ()
  "Custom column displayers should match, fall back, and truncate predictably."
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
               "state:7"))))
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
              (should (string-match-p "failed: boom" logged))))))))
  (dolist (case '(default custom-displayer))
    (ert-info ((format "truncate: %s" case))
      (let ((clutch-column-displayers nil))
        (when (eq case 'custom-displayer)
          (clutch-register-column-displayer
           "orders" "status"
           (lambda (_value)
             "abcdef")))
        (with-temp-buffer
          (setq-local clutch--result-source-table "orders")
          (should (equal
                   (clutch--cell-display-content
                    (if (eq case 'custom-displayer) "queued" "abcdef")
                    4 '(:name "status" :type-category text) nil)
                   "abc…")))))))

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

(ert-deftest clutch-test-cell-display-content-structured-text-contract ()
  "Structured JSON/XML cells should highlight short text and truncate long text."
  (let ((s (clutch--cell-display-content
            "{\"a\":1,\"b\":\"x\"}" 20 '(:name "payload" :type-category json) nil)))
    (should (equal (substring-no-properties s) "{\"a\":1,\"b\":\"x\"}"))
    (should-not (get-text-property 0 'clutch-cell-truncated s))
    (should (eq (get-text-property 0 'face s) 'shadow))
    (should (eq (get-text-property 1 'face s) 'font-lock-property-name-face))
    (should-not (eq (get-text-property 1 'face s) 'clutch-field-name-face))
    (should (eq (get-text-property 5 'face s) 'font-lock-constant-face))
    (should (eq (get-text-property 12 'face s) 'font-lock-string-face)))
  (dolist (case '(("long JSON" "{\"status\":\"paid\",\"total\":128.5}"
                   (:name "payload" :type-category json) nil "<JSON>")
                  ("JSON blob" "{\"status\":\"paid\",\"total\":128.5}"
                   (:name "payload" :type-category blob) "{\"status\"" "<BLOB>")
                  ("long XML" "<order><item sku=\"A1\"/><item sku=\"B2\"/></order>"
                   (:name "payload" :type-category text) "<order>" "<XML>")))
    (pcase-let ((`(,label ,value ,column ,prefix ,placeholder) case))
      (ert-info ((format "case: %s" label))
        (let ((s (clutch--cell-display-content value 18 column nil)))
          (when prefix
            (should (string-prefix-p prefix s)))
          (should (string-suffix-p "…" s))
          (should (get-text-property 0 'clutch-cell-truncated s))
          (should-not (string-match-p placeholder s))
          (should-not (get-text-property 0 'face s))))))
  (let ((s (clutch--cell-display-content
            "<root attr=\"x\"><a>1</a></root>"
            40 '(:name "payload" :type-category text) nil)))
    (should (equal (substring-no-properties s)
                   "<root attr=\"x\"><a>1</a></root>"))
    (should-not (get-text-property 0 'clutch-cell-truncated s))
    (should (eq (get-text-property 0 'face s) 'shadow))
    (should (eq (get-text-property 1 'face s) 'font-lock-function-name-face))
    (should (eq (get-text-property 6 'face s) 'font-lock-property-name-face))
    (should (eq (get-text-property 11 'face s) 'font-lock-string-face))
    (should (eq (get-text-property 20 'face s) 'shadow))))

(ert-deftest clutch-test-render-row-custom-displayer-keeps-full-value-raw ()
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
      (let ((cell (clutch--render-row '(2) 0 '(0) [6] nil)))
        (should (string-match-p "done" cell))
        (should (= (get-text-property 2 'clutch-full-value cell) 2))))))

;;;; Rendering — automatic child-frame cell preview

(ert-deftest clutch-test-cell-preview-is-opt-in-and-keeps-full-viewers ()
  "Automatic previews should default off without replacing the v viewers."
  (should-not (default-value 'clutch-cell-preview-style))
  (should (eq (lookup-key clutch-result-mode-map (kbd "v"))
              #'clutch-result-view-value))
  (should (eq (lookup-key clutch-record-mode-map (kbd "v"))
              #'clutch-record-view-value))
  (should-not (lookup-key clutch-result-mode-map (kbd "V")))
  (should-not (lookup-key clutch-record-mode-map (kbd "V")))
  (let ((clutch-cell-preview-style 'child-frame)
        (clutch--cell-preview-state nil))
    (with-temp-buffer
      (clutch-result-mode)
      (cl-letf (((symbol-function 'clutch--cell-preview-supported-p)
                 (lambda (_window) nil))
                ((symbol-function 'run-with-idle-timer)
                 (lambda (&rest _args)
                   (ert-fail "Unsupported displays must not schedule previews"))))
        (clutch--schedule-cell-preview)
        (should-not clutch--cell-preview-timer)
        (should-not clutch--cell-preview-state)))))

(ert-deftest clutch-test-cell-preview-coalesces-rapid-navigation ()
  "Rapid cell movement should render only the final scheduled cell."
  (let ((source (generate-new-buffer " *clutch-preview-source*"))
        (clutch-cell-preview-style 'child-frame)
        (clutch--cell-preview-state nil)
        scheduled cancelled rendered
        (sequence 0))
    (unwind-protect
        (save-window-excursion
          (set-window-buffer (selected-window) source)
          (with-current-buffer source
            (clutch-test--init-result-state
             '(:source-table "cases"
               :column-defs ((:name "id" :type-category numeric)
                             (:name "name" :type-category text))
               :column-widths [2 5]
               :rows ((1 "alice-long-value") (2 "bob-long-value"))))
            (clutch--refresh-display)
            (goto-char (point-min)))
          (cl-letf (((symbol-function 'clutch--cell-preview-supported-p)
                     (lambda (_window) t))
                    ((symbol-function 'run-with-idle-timer)
                     (lambda (_seconds _repeat function &rest args)
                       (let ((token (list 'timer (cl-incf sequence))))
                         (setq scheduled (list token function args))
                         token)))
                    ((symbol-function 'cancel-timer)
                     (lambda (timer) (push timer cancelled)))
                    ((symbol-function 'clutch--open-cell-preview)
                     (lambda (_source _window context)
                       (push (plist-get context :value) rendered)
                       t)))
            (with-current-buffer source
              (when-let* ((match (text-property-search-forward
                                  'clutch-col-idx 0 #'eq)))
                (goto-char (prop-match-beginning match)))
              (clutch--schedule-cell-preview)
              (should-not clutch--cell-preview-timer)
              (goto-char (point-min))
              (when-let* ((match (text-property-search-forward
                                  'clutch-col-idx 1 #'eq)))
                (goto-char (prop-match-beginning match)))
              (clutch--schedule-cell-preview)
              (let ((first-timer clutch--cell-preview-timer))
                (clutch-result-down-cell)
                (clutch--schedule-cell-preview)
                (should (member first-timer cancelled)))
              (apply (nth 1 scheduled) (nth 2 scheduled))
              (should (equal rendered '("bob-long-value")))
              (should-not clutch--cell-preview-timer))))
      (when (buffer-live-p source)
        (kill-buffer source)))))

(ert-deftest clutch-test-cell-preview-waits-and-hides-between-cells ()
  "Changing cells should hide stale content until the preview delay passes."
  (let* ((source (current-buffer))
         (source-window (selected-window))
         (clutch-cell-preview-style 'child-frame)
         (clutch-cell-preview-delay 0.25)
         (clutch--cell-preview-state
          (list :source-buffer source
                :source-window source-window
                :frame 'preview
                :cell-id '(1 0 0)))
         (clutch--cell-preview-timer nil)
         hidden scheduled)
    (cl-letf (((symbol-function 'clutch--cell-preview-supported-p)
               (lambda (_window) t))
              ((symbol-function 'clutch--cell-preview-context)
               (lambda ()
                 '(:cell-id (1 0 1) :row-index 0 :column-index 1)))
              ((symbol-function 'frame-live-p) (lambda (_frame) t))
              ((symbol-function 'make-frame-invisible)
               (lambda (frame) (setq hidden frame)))
              ((symbol-function 'run-with-idle-timer)
               (lambda (delay repeat function &rest args)
                 (setq scheduled (list delay repeat function args))
                 'new-timer)))
      (clutch--schedule-cell-preview))
    (should (eq hidden 'preview))
    (should (eq clutch--cell-preview-timer 'new-timer))
    (should (= (car scheduled) clutch-cell-preview-delay))
    (should-not (nth 1 scheduled))))

(ert-deftest clutch-test-cell-preview-cleans-up-nonlocal-exits-and-buffer-kills ()
  "Preview creation and external buffer kills should not leak global state."
  (let ((source (current-buffer))
        (source-window (selected-window))
        (clutch--cell-preview-state nil)
        deleted)
    (cl-letf (((symbol-function 'clutch--make-cell-preview-frame)
               (lambda (_buffer _parent) 'preview-frame))
              ((symbol-function 'frame-live-p)
               (lambda (frame) (eq frame 'preview-frame)))
              ((symbol-function 'delete-frame)
               (lambda (_frame &optional _force) (setq deleted t)))
              ((symbol-function 'clutch--render-cell-preview)
               (lambda (_context) (signal 'quit nil))))
      (let (quit-seen)
        (condition-case nil
            (clutch--open-cell-preview source source-window '(:value "x"))
          (quit (setq quit-seen t)))
        (should quit-seen))
      (should deleted)
      (should-not clutch--cell-preview-state)
      (should-not (get-buffer " *clutch-cell-preview*"))
      (should-not (memq #'clutch--cell-preview-lifecycle-post-command
                        (default-value 'post-command-hook)))
      (should-not (memq #'clutch--cell-preview-window-size-change
                        window-size-change-functions)))
    (setq deleted nil)
    (cl-letf (((symbol-function 'clutch--make-cell-preview-frame)
               (lambda (_buffer _parent) 'preview-frame))
              ((symbol-function 'frame-live-p)
               (lambda (frame) (eq frame 'preview-frame)))
              ((symbol-function 'delete-frame)
               (lambda (_frame &optional _force) (setq deleted t)))
              ((symbol-function 'clutch--render-cell-preview)
               (lambda (_context)
                 (with-current-buffer " *clutch-cell-preview*"
                   (add-hook 'kill-buffer-hook
                             #'clutch--close-cell-preview nil t)))))
      (should (clutch--open-cell-preview
               source source-window '(:cell-id (1 0 0) :value "x")))
      (kill-buffer " *clutch-cell-preview*")
      (should deleted)
      (should-not clutch--cell-preview-state)
      (should-not (memq #'clutch--cell-preview-lifecycle-post-command
                        (default-value 'post-command-hook)))
      (should-not (memq #'clutch--cell-preview-window-size-change
                        window-size-change-functions)))))

(ert-deftest clutch-test-cell-preview-size-and-position-are-bounded ()
  "Cell previews should be distinct, bounded, and above the minibuffer."
  (cl-letf (((symbol-function 'face-background)
             (lambda (&rest _args) "#202020"))
            ((symbol-function 'face-foreground)
             (lambda (&rest _args) "#f2f2f2"))
            ((symbol-function 'color-name-to-rgb)
             (lambda (_color) '(0.1 0.1 0.1)))
            ((symbol-function 'color-dark-p)
             (lambda (_rgb) t))
            ((symbol-function 'color-lighten-name)
             (lambda (color amount)
               (format "%s+%d" color amount))))
    (should
     (equal (clutch--cell-preview-colors (selected-frame))
            '("#202020+10" "#f2f2f2" "#202020+32"))))
  (let* ((clutch-cell-preview-max-size '(0.5 . 0.25))
         (frame (selected-frame))
         (limits (clutch--cell-preview-size-limits frame frame)))
    (should (= (nth 1 limits) 1))
    (should (= (nth 3 limits) 1))
    (should (= (nth 0 limits)
               (max 1 (floor (/ (* (frame-text-height frame) 0.25)
                                  (window-default-line-height
                                   (frame-root-window frame)))))))
    (should (= (nth 2 limits)
               (max 1 (floor (/ (* (frame-text-width frame) 0.5)
                                  (frame-char-width frame)))))))
  (dolist (case '((20 150 20 300 200 1000 760 (20 . 176))
                  (1000 750 20 300 200 1000 760 (694 . 544))))
    (pcase-let ((`(,x ,y ,line-height ,width ,height
                      ,parent-width ,bottom ,expected)
                 case))
      (should (equal (clutch--cell-preview-coordinates
                      x y line-height width height parent-width bottom)
                     expected)))))

(ert-deftest clutch-test-cell-preview-allows-one-line-frame-height ()
  "Preview fitting should override the global four-line window minimum."
  (let (call)
    (cl-letf (((symbol-function 'frame-parent)
               (lambda (_frame) 'parent))
              ((symbol-function 'clutch--cell-preview-size-limits)
               (lambda (_parent _frame) '(12 1 80 1)))
              ((symbol-function 'fit-frame-to-buffer)
               (lambda (&rest args)
                 (setq call (list args window-min-height window-min-width)))))
      (clutch--fit-cell-preview-frame 'preview))
    (should (equal call '((preview 12 1 80 1) 1 1)))))

(ert-deftest clutch-test-cell-preview-window-resize-reschedules-preview ()
  "Parent resize should coalesce through the existing preview scheduler."
  (let* ((source (current-buffer))
         (source-window 'source-window)
         (clutch--cell-preview-state
          (list :source-buffer source
                :source-window source-window))
         (clutch--cell-preview-timer 'old-timer)
         cancelled scheduled refreshed)
    (cl-letf (((symbol-function 'window-live-p) (lambda (_window) t))
              ((symbol-function 'window-frame) (lambda (_window) 'parent))
              ((symbol-function 'cancel-timer)
               (lambda (timer) (setq cancelled timer)))
              ((symbol-function 'run-with-idle-timer)
               (lambda (delay repeat function &rest args)
                 (setq scheduled (list delay repeat function args))
                 'new-timer)))
      (clutch--cell-preview-window-size-change 'other)
      (should-not scheduled)
      (clutch--cell-preview-window-size-change 'parent))
    (should (eq cancelled 'old-timer))
    (should (eq clutch--cell-preview-timer 'new-timer))
    (pcase-let ((`(,delay ,repeat ,function ,args) scheduled))
      (should (= delay 0.1))
      (should-not repeat)
      (cl-letf (((symbol-function 'clutch--schedule-cell-preview)
                 (lambda () (setq refreshed (current-buffer)))))
        (apply function args)))
    (should (eq refreshed source))
    (should-not clutch--cell-preview-timer)))

;;;; Shell command on cell

(ert-deftest clutch-test-shell-command-on-cell-pipes-value-and-requires-cell ()
  "Shell commands should pipe the current cell value and reject non-cell points."
  (dolist (case `(("hello world" "hello world")
                  (42 "42")
                  (,clutch--cell-default-placeholder "<default>")))
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
              (should (string-match-p expected captured))))))))
  (with-temp-buffer
    (should-error (clutch-result-shell-command-on-cell "cat") :type 'user-error)))

;;;; Schema cache — refresh and status

(ert-deftest clutch-test-metadata-cache-uses-connection-identity ()
  "Structurally equal connection tokens must keep distinct metadata state."
  (clutch-test--with-isolated-metadata-caches
   (let ((conn-a (list 'same-connection-shape))
         (conn-b (list 'same-connection-shape)))
     (should (equal conn-a conn-b))
     (should-not (eq conn-a conn-b))
     (clutch--set-table-metadata conn-a "users" :column-details '(id))
     (should (equal (plist-get (clutch--table-metadata conn-a "users")
                               :column-details)
                    '(id)))
     (should-not (clutch--table-metadata conn-b "users")))))

(ert-deftest clutch-test-object-warmup-generations-do-not-own-connections ()
  "Warmup freshness tracking must not keep retired connections alive."
  (should (eq (hash-table-weakness clutch--object-warmup-generations)
              'key)))

(ert-deftest clutch-test-object-warmup-error-advances-past-failed-category ()
  "A permanent category error should not retry forever or starve later work."
  (let ((clutch--object-cache (make-hash-table :test 'eq))
        (conn 'warmup-conn)
        scheduled)
    (cl-letf (((symbol-function 'clutch--object-warmup-current-p)
               (lambda (_conn _generation) t))
              ((symbol-function 'clutch--object-warmup-debug-event) #'ignore)
              ((symbol-function 'clutch--browseable-object-entries)
               (lambda (_conn) nil))
              ((symbol-function 'clutch--cache-table-entry-comments) #'ignore)
              ((symbol-function 'clutch--schedule-object-warmup)
               (lambda (_conn) (setq scheduled t))))
      (clutch--object-warmup-error
       conn 3 'indexes "permission denied")
      (should (memq 'indexes
                    (clutch--object-cache-loaded-categories conn)))
      (should scheduled))))

(ert-deftest clutch-test-refresh-schema-cache-records-ready-status ()
  "Schema refresh entry points should record ready state and table count."
  (dolist (mode '(sync async))
    (ert-info ((format "mode: %s" mode))
      (clutch-test--with-isolated-metadata-caches
       (cl-letf (((symbol-function 'clutch-db-list-tables)
                  (lambda (_conn) '("users" "orders")))
                 ((symbol-function 'clutch-db-live-p)
                  (lambda (_conn) t))
                 ((symbol-function 'clutch-db-refresh-schema-async)
                  (lambda (_conn callback &optional _errback _idle-delay)
                    (funcall callback '("users" "orders"))
                    t)))
         (should (pcase mode
                   ('sync (clutch--refresh-schema-cache 'fake-conn))
                   ('async (clutch--refresh-schema-cache-async 'fake-conn))))
         (let ((status (gethash 'fake-conn clutch--schema-status-cache)))
           (should (eq (plist-get status :state) 'ready))
           (should (= (plist-get status :tables) 2))))))))

(ert-deftest clutch-test-refresh-schema-cache-propagates-programmer-errors ()
  "Synchronous schema refresh should not hide non-database failures."
  (clutch-test--with-isolated-metadata-caches
   (cl-letf (((symbol-function 'clutch-db-list-tables)
              (lambda (_conn)
                (signal 'wrong-type-argument '(integerp broken-state)))))
     (should-error (clutch--refresh-schema-cache 'fake-conn)
                   :type 'wrong-type-argument))))

(ert-deftest clutch-test-refresh-schema-cache-async-callback-contract ()
  "Async schema refresh should ignore stale callbacks and trace callback phases."
  (clutch-test--with-isolated-metadata-caches
   (let ((conn (make-clutch-db-sqlite-conn :database "/tmp/debug.db"))
         first-callback second-callback)
     (with-temp-buffer
       (let ((clutch-debug-mode t))
         (setq-local clutch-connection conn)
         (cl-letf (((symbol-function 'clutch-db-live-p)
                    (lambda (_conn) t))
                   ((symbol-function 'clutch-db-backend-key)
                    (lambda (_conn) 'mysql))
                   ((symbol-function 'clutch-db-refresh-schema-async)
                    (lambda (_conn callback &optional _errback _idle-delay)
                      (if first-callback
                          (setq second-callback callback)
                        (setq first-callback callback))
                      t))
                   ((symbol-function 'pop-to-buffer)
                    (lambda (buf &rest _args) buf)))
           (clutch--clear-debug-capture)
           (should (clutch--refresh-schema-cache-async conn))
           (should (clutch--refresh-schema-cache-async conn))
           (funcall first-callback '("stale_users"))
           (should-not (gethash conn clutch--schema-cache))
           (funcall second-callback '("users" "orders"))
           (should (= (hash-table-count
                       (gethash conn clutch--schema-cache))
                      2))
           (should (eq (plist-get
                        (gethash conn clutch--schema-status-cache)
                                  :state)
                       'ready))
           (let ((text (clutch-test--debug-buffer-string)))
             (should (string-match-p "Operation: schema-refresh" text))
             (should (string-match-p "Phase: submit" text))
             (should (string-match-p "Phase: success" text))
             (should (string-match-p "Phase: stale-drop" text)))))))))

(ert-deftest clutch-test-refresh-schema-cache-async-records-current-closed-error ()
  "Async schema refresh should finish current closed-connection errors."
  (clutch-test--with-isolated-metadata-caches
   (let ((alive t)
         errback
         problem)
     (cl-letf (((symbol-function 'clutch-db-live-p)
                (lambda (_conn) alive))
               ((symbol-function 'clutch--remember-problem-record)
                (lambda (&rest args) (setq problem args)))
               ((symbol-function 'clutch-db-refresh-schema-async)
                (lambda (_conn _callback captured-errback &optional _idle-delay)
                  (setq errback captured-errback)
                  t)))
       (should (clutch--refresh-schema-cache-async 'fake-conn))
       (should (eq (plist-get
                    (gethash 'fake-conn clutch--schema-status-cache)
                              :state)
                   'refreshing))
       (setq alive nil)
       (funcall errback "Connection closed")
       (let ((status (gethash 'fake-conn clutch--schema-status-cache)))
         (should (eq (plist-get status :state) 'failed))
         (should (equal (plist-get status :error) "Connection closed")))
       (should problem)))))

(ert-deftest clutch-test-console-buffer-name-reflects-schema-status ()
  "Console buffer names should expose schema status."
  (let ((clutch--schema-status-cache (make-hash-table :test 'eq)))
    (with-temp-buffer
      (clutch-mode)
      (setq-local clutch--console-name "dev"
                  clutch-connection 'fake-conn)
      (puthash 'fake-conn '(:state stale) clutch--schema-status-cache)
      (clutch--update-console-buffer-name)
      (should (equal (buffer-name) "*clutch: dev* [schema~]"))
      (puthash 'fake-conn '(:state refreshing) clutch--schema-status-cache)
      (clutch--update-console-buffer-name)
      (should (equal (buffer-name) "*clutch: dev* [schema...]"))
      (puthash 'fake-conn '(:state ready :tables 42)
               clutch--schema-status-cache)
      (clutch--update-console-buffer-name)
      (should (equal (buffer-name) "*clutch: dev* [schema 42t]")))))

(ert-deftest clutch-test-schema-state-header-line-segment ()
  "Schema states should produce the correct header-line segment text."
  (should (equal (clutch--schema-state-header-line-segment 'stale)
                 (propertize "schema~" 'face 'warning)))
  (should (equal (clutch--schema-state-header-line-segment 'failed)
                 (propertize "schema!" 'face 'error)))
  (should (equal (clutch--schema-state-header-line-segment 'refreshing)
                 (propertize "schema…" 'face 'shadow))))

(ert-deftest clutch-test-refresh-current-schema-background-contract ()
  "Manual schema refresh should pick the background or synchronous path.
Lazy backends refresh in the background and fall back to a synchronous
refresh, while the `clutch-refresh-schema' command always refreshes
synchronously."
  (dolist (case
           (list
            (list "background" (lambda () (clutch--refresh-current-schema))
                  t t nil "started in background")
            (list "fallback" (lambda () (clutch--refresh-current-schema))
                  nil t t "Schema refreshed (2 tables)")
            (list "command entry point forces sync"
                  (lambda () (clutch-refresh-schema))
                  t nil t "Schema refreshed (2 tables)")))
    (pcase-let ((`(,label ,entry-point ,async-result ,expect-async
                   ,expect-sync ,message-fragment)
                 case))
      (ert-info ((format "case: %s" label))
        (let ((clutch--schema-status-cache (make-hash-table :test 'eq))
              seen-message
              sync-called
              async-called)
          (cl-letf (((symbol-function 'clutch-db-live-p)
                     (lambda (_conn) t))
                    ((symbol-function 'clutch-db-eager-schema-refresh-p)
                     (lambda (_conn) nil))
                    ((symbol-function 'clutch--refresh-schema-cache-async)
                     (lambda (_conn)
                       (setq async-called t)
                       async-result))
                    ((symbol-function 'clutch--refresh-schema-cache)
                     (lambda (_conn)
                       (setq sync-called t)
                       (puthash 'fake-conn '(:state ready :tables 2)
                                clutch--schema-status-cache)
                       t))
                    ((symbol-function 'message)
                     (lambda (fmt &rest args)
                       (setq seen-message (apply #'format fmt args)))))
            (with-temp-buffer
              (setq-local clutch-connection 'fake-conn)
              (should (funcall entry-point))
              (if expect-async
                  (should async-called)
                (should-not async-called))
              (if expect-sync
                  (should sync-called)
                (should-not sync-called))
              (should (string-match-p (regexp-quote message-fragment)
                                      seen-message)))))))))

(ert-deftest clutch-test-refresh-schema-failure-message-shows-error-as-is ()
  "A failed schema refresh should show the error text as it is.
MySQL access errors name the host pattern, as in \\='u\\='@\\='%\\='."
  (let ((clutch--schema-status-cache (make-hash-table :test 'eq))
        (error-text "Access denied for user 'u'@'%' to database 'zj'")
        seen-message)
    (cl-letf (((symbol-function 'clutch-db-live-p)
               (lambda (_conn) t))
              ((symbol-function 'clutch--refresh-schema-cache)
               (lambda (conn)
                 (puthash conn (list :state 'failed :error error-text)
                          clutch--schema-status-cache)
                 nil))
              ((symbol-function 'message)
               (lambda (fmt &rest args)
                 (setq seen-message (apply #'format-message fmt args)))))
      (with-temp-buffer
        (setq-local clutch-connection 'fake-conn)
        (should-not (clutch-refresh-schema))
        (should (equal seen-message
                       (concat "Schema refresh failed: " error-text)))))))

(ert-deftest clutch-test-describe-dwim-warns-when-schema-cache-is-stale ()
  "Object prompts should surface stale-schema recovery hints."
  (let ((clutch--schema-status-cache (make-hash-table :test 'eq))
        hinted
        described)
    (cl-letf (((symbol-function 'clutch-db-live-p)
               (lambda (_conn) t))
              ((symbol-function 'clutch--object-entries)
               (lambda (_conn)
                 '((:name "users" :type "TABLE"))))
              ((symbol-function 'clutch--object-entry-reader)
               (lambda (_conn _prompt entries &rest _)
                 (car entries)))
              ((symbol-function 'clutch-object-describe)
               (lambda (entry)
                 (setq described entry)))
              ((symbol-function 'message)
               (lambda (fmt &rest args)
                 (setq hinted (apply #'format fmt args)))))
      (puthash 'fake-conn '(:state stale) clutch--schema-status-cache)
      (with-temp-buffer
        (setq-local clutch-connection 'fake-conn)
        (call-interactively #'clutch-describe-dwim))
      (should (equal described '(:name "users" :type "TABLE")))
      (should (string-match-p "Schema cache is stale" hinted)))))

;;;; Schema cache — column details and metadata

(ert-deftest clutch-test-result-column-info-works-on-cell-padding ()
  "Column info should resolve from padded whitespace inside a data cell."
  (with-temp-buffer
    (setq-local clutch-column-padding 1
                clutch--result-columns '("name")
                clutch--result-column-defs '((:name "name" :type-category text))
                clutch--result-column-details
                (list (list :name "name" :type "VARCHAR(255)" :nullable t)))
    (insert (clutch--render-row '("alice") 0 '(0) [8] nil))
    (goto-char 2)
    (let (seen)
      (cl-letf (((symbol-function 'message)
                 (lambda (fmt &rest args)
                   (setq seen (apply #'format fmt args)))))
        (clutch-result-column-info)
        (should (string-match-p "name" seen))
        (should (string-match-p "Type: VARCHAR(255)" seen))))))

(ert-deftest clutch-test-metadata-sync-failures-are-memoized ()
  "Repeated sync metadata failures should not reissue the same failing RPC."
  (clutch-test--with-isolated-metadata-caches
   (let ((schema (make-hash-table :test 'equal)) (column-calls 0)
         (detail-calls 0))
     (puthash "users" nil schema)
     (cl-letf (((symbol-function 'clutch-db-list-columns)
		(lambda (_conn _table)
                  (cl-incf column-calls)
                  (signal 'clutch-db-error '("column load failed"))))
               ((symbol-function 'clutch-db-column-details)
		(lambda (_conn _table)
                  (cl-incf detail-calls)
                  (signal 'clutch-db-error '("detail load failed")))))
       (dotimes (_ 2)
         (should-not (clutch--ensure-columns 'fake-conn schema "users"))
         (should-not (clutch--ensure-column-details 'fake-conn "users")))
       (should (= column-calls detail-calls 1))
       (dolist (property '(:columns-status :column-details-status))
         (should (eq (plist-get (clutch--metadata-status
                                 'fake-conn "users" property) :state)
                     'failed)))))))

(ert-deftest clutch-test-transient-metadata-errors-are-not-cached ()
  "Transient metadata failures should stay observable and retryable."
  (clutch-test--with-isolated-metadata-caches
   (let ((calls (make-hash-table :test 'eq))
         warnings)
     (cl-letf (((symbol-function 'clutch-db-table-comment)
		(lambda (_conn _table &optional _schema)
                  (cl-incf (gethash 'comment calls 0))
                  (if (= (gethash 'comment calls) 1)
                      (signal 'clutch-db-error '("comment boom"))
                    "Orders table")))
               ((symbol-function 'clutch-db-symbol-help)
		(lambda (_conn _symbol)
                  (cl-incf (gethash 'help calls 0))
                  (if (= (gethash 'help calls) 1)
                      (signal 'clutch-db-error '("help boom"))
                    '(:sig "ABS(X)" :desc "Returns absolute value."))))
               ((symbol-function 'clutch--remember-recoverable-metadata-warning)
                (lambda (_conn op _err &optional context)
                  (push (list op context) warnings))))
       (dolist (case `((comment
			,(lambda ()
                           (clutch--ensure-table-comment 'fake-conn "orders"))
			"Orders table"
			equal)
                       (help
			,(lambda ()
                           (clutch--ensure-help-doc 'fake-conn "abs"))
			"ABS(X)"
			string-match-p)))
         (pcase-let ((`(,label ,load ,expected ,match) case))
           (ert-info ((format "case: %s" label))
             (should-not (funcall load))
             (should (funcall match expected (funcall load)))
             (should (= (gethash label calls) 2)))))
       (should (equal (sort warnings
                            (lambda (a b) (string< (car a) (car b))))
                      '(("symbol help" (:symbol "abs"))
                        ("table comment" (:table "orders" :schema nil)))))))))

(ert-deftest clutch-test-column-details-async-callback-lifecycle ()
  "Async details should reject invalid callbacks and retain empty results."
  (dolist (case '(stale-ticket cleared-active dead-connection empty-result))
    (clutch-test--with-isolated-metadata-caches
     (let ((alive t) callback (calls 0))
       (cl-letf (((symbol-function 'clutch-db-live-p)
                  (lambda (_conn) alive))
                 ((symbol-function 'clutch-db-column-details-async)
                  (lambda (_conn _table cb &optional _errback)
                    (cl-incf calls)
                    (setq callback cb)
                    t)))
         (clutch--ensure-column-details-async 'fake-conn "users")
         (pcase case
           ((or 'stale-ticket 'cleared-active)
            (clutch--ensure-column-details-async 'fake-conn "orders")
            (if (eq case 'stale-ticket)
                (clutch--set-metadata-status
                 'fake-conn "users" :column-details-status 'loading nil
                 (clutch--begin-metadata-ticket))
              (clutch--clear-table-metadata-caches 'fake-conn "users")))
           ('dead-connection (setq alive nil)))
         (funcall callback
                  (unless (eq case 'empty-result)
                    '((:name "ignored" :type "int"))))
         (if (eq case 'empty-result)
             (progn
               (should (clutch--column-details-cached-p 'fake-conn "users"))
               (should-not (clutch--cached-column-details 'fake-conn "users"))
               (clutch--ensure-column-details-async 'fake-conn "users")
               (should (= calls 1)))
           (should-not (clutch--column-details-cached-p 'fake-conn "users")))
         (when (memq case '(stale-ticket cleared-active))
           (should (equal (car (clutch--column-details-active 'fake-conn))
                          "orders"))))))))

(ert-deftest clutch-test-load-fk-info-is-cache-first-and-async ()
  "Result FK display metadata should not synchronously hit the backend."
  (clutch-test--with-isolated-metadata-caches
   (with-temp-buffer
     (clutch-result-mode)
     (setq-local clutch-connection 'fake-conn
                 clutch--result-source-table "users"
                 clutch--result-columns '("id" "account_id"))
     (let (queued)
       (cl-letf (((symbol-function 'clutch-db-foreign-keys)
                  (lambda (&rest _)
                    (error "foreign keys should not load synchronously")))
                 ((symbol-function 'clutch--ensure-foreign-keys-async)
                  (lambda (_conn table)
                    (setq queued table))))
         (clutch--load-fk-info)
         (should (equal queued "users"))
         (should-not clutch--fk-info))))))

(ert-deftest clutch-test-foreign-keys-async-caches-and-refreshes-results ()
  "Foreign-key async callbacks should cache metadata and notify listeners."
  (clutch-test--with-isolated-metadata-caches
   (let (callback notified)
     (let ((clutch--table-metadata-updated-hook
            (list (lambda (conn table kind)
                    (should-not (clutch--metadata-status conn table :foreign-keys-status))
                    (setq notified (list conn table kind))))))
       (cl-letf (((symbol-function 'clutch-db-live-p)
                  (lambda (_conn) t))
                 ((symbol-function 'clutch-db-foreign-keys-async)
                  (lambda (_conn _table cb &optional _errback)
                    (setq callback cb)
                    t)))
         (clutch--ensure-foreign-keys-async 'fake-conn "users")
         (funcall callback '(("account_id" :ref-table "accounts"
                              :ref-column "id")))
         (should (equal (clutch--cached-foreign-keys 'fake-conn "users")
                        '(("account_id" :ref-table "accounts"
                           :ref-column "id"))))
         (should (equal notified '(fake-conn "users" foreign-keys)))
         (should-not (clutch--metadata-status 'fake-conn "users" :foreign-keys-status)))))))

(ert-deftest clutch-test-foreign-keys-async-unsupported-caches-empty-result ()
  "Backends without async foreign-key metadata should not be retried on each render."
  (clutch-test--with-isolated-metadata-caches
   (let ((calls 0))
     (cl-letf (((symbol-function 'clutch-db-foreign-keys-async)
                (lambda (&rest _args)
                  (cl-incf calls)
                  nil)))
       (clutch--ensure-foreign-keys-async 'fake-conn "users")
       (clutch--ensure-foreign-keys-async 'fake-conn "users")
       (should (= calls 1))
       (should (clutch--foreign-keys-cached-p 'fake-conn "users"))
       (should-not (clutch--cached-foreign-keys 'fake-conn "users"))
       (should-not (clutch--metadata-status 'fake-conn "users" :foreign-keys-status))))))

(ert-deftest clutch-test-refresh-result-metadata-buffers-updates-only-matching-results ()
  "Result metadata refresh should match connection identity, not its label."
  (let ((conn-a (list 'same-connection-shape))
        (conn-b (list 'same-connection-shape))
        (buf-a (generate-new-buffer " *clutch-result-a*"))
        (buf-b (generate-new-buffer " *clutch-result-b*"))
        (details '((:name "id" :type "int"))))
    (unwind-protect
        (cl-letf (((symbol-function 'clutch--connection-key)
                   (lambda (_conn) "same-label"))
                  ((symbol-function 'clutch--result-column-details)
                   (lambda (_conn _table _col-names)
                     details)))
          (should (equal conn-a conn-b))
          (should-not (eq conn-a conn-b))
          (with-current-buffer buf-a
            (clutch-result-mode)
            (setq-local clutch-connection conn-a)
            (setq-local clutch--result-columns '("id"))
            (setq-local clutch--result-source-table "users")
            (setq-local clutch--last-query "select * from users"))
          (with-current-buffer buf-b
            (clutch-result-mode)
            (setq-local clutch-connection conn-b)
            (setq-local clutch--result-columns '("id"))
            (setq-local clutch--result-source-table "users")
            (setq-local clutch--last-query "select * from users"))
          (clutch--handle-table-metadata-updated conn-a "users" 'column-details)
          (with-current-buffer buf-a
            (should (equal clutch--result-column-details details)))
          (with-current-buffer buf-b
            (should-not clutch--result-column-details)))
      (when (buffer-live-p buf-a)
        (kill-buffer buf-a))
      (when (buffer-live-p buf-b)
        (kill-buffer buf-b)))))

(ert-deftest clutch-test-metadata-update-refreshes-only-its-qualified-table ()
  "Column details of aux.people should refresh results of aux.people only."
  (let ((plain (generate-new-buffer " *clutch-result-plain*"))
        (aux (generate-new-buffer " *clutch-result-aux*")))
    (unwind-protect
        (cl-letf (((symbol-function 'clutch--result-column-details)
                   (lambda (_conn table _col-names) (list table))))
          (dolist (spec (list (list plain nil) (list aux "aux")))
            (with-current-buffer (car spec)
              (clutch-result-mode)
              (setq-local clutch-connection 'conn
                          clutch--result-columns '("id")
                          clutch--result-source-table "people"
                          clutch--result-source-schema (cadr spec)
                          clutch--last-query "select * from people")))
          (clutch--handle-table-metadata-updated
           'conn '(nil "aux" "people") 'column-details)
          (with-current-buffer aux
            (should (equal clutch--result-column-details
                           '((nil "aux" "people")))))
          (with-current-buffer plain
            (should-not clutch--result-column-details)))
      (kill-buffer plain)
      (kill-buffer aux))))

(ert-deftest clutch-test-column-details-refresh-redraws-pending-insert-placeholders ()
  "Async column details should redraw staged insert metadata placeholders."
  (clutch-test--with-isolated-metadata-caches
   (let ((buf (generate-new-buffer " *clutch-result-pending-insert*"))
         callback)
     (unwind-protect
         (let ((clutch--table-metadata-updated-hook
                (list #'clutch--handle-table-metadata-updated)))
           (cl-letf (((symbol-function 'clutch-db-live-p)
                      (lambda (_conn) t))
                     ((symbol-function 'clutch-db-column-details-async)
                      (lambda (_conn _table cb &optional _errback)
                        (setq callback cb)
                        t)))
             (with-current-buffer buf
               (clutch-test--init-result-state
                (list :connection 'fake-conn
                      :columns '("id" "name")
                      :column-defs '((:name "id" :type-category numeric)
                                     (:name "name" :type-category text))
                      :rows nil
                      :source-table "users"
                      :pending-inserts '((("name" . "alice")))
                      :column-widths [12 12]
                      :render t))
               (should-not (string-match-p "<generated>" (buffer-string))))
             (should callback)
             (funcall callback '((:name "id" :generated t)
                                 (:name "name")))
             (with-current-buffer buf
               (should (string-match-p "<generated>" (buffer-string)))
               (should (string-match-p "alice" (buffer-string))))))
       (when (buffer-live-p buf)
         (kill-buffer buf))))))

(ert-deftest clutch-test-foreign-key-refresh-redraws-only-marked-results ()
  "Async foreign keys should redraw a result only when they mark its columns."
  (dolist (case '(unmarked marked))
    (clutch-test--with-isolated-metadata-caches
     (let ((buf (generate-new-buffer " *clutch-result-fk*"))
           (render (symbol-function 'clutch--render-result))
           (renders 0)
           callback)
       (unwind-protect
           (let ((clutch--table-metadata-updated-hook
                  (list #'clutch--handle-table-metadata-updated)))
             (cl-letf (((symbol-function 'clutch-db-live-p)
                        (lambda (_conn) t))
                       ((symbol-function 'clutch-db-foreign-keys-async)
                        (lambda (_conn _table cb &optional _errback)
                          (setq callback cb)
                          t)))
               (with-current-buffer buf
                 (clutch-test--init-result-state
                  (list :connection 'fake-conn
                        :columns '("id" "account_id")
                        :rows '((1 7))
                        :source-table "users"
                        :column-widths [12 12]
                        :render t))
                 (clutch--load-fk-info))
               (should callback)
               (cl-letf (((symbol-function 'clutch--render-result)
                          (lambda ()
                            (cl-incf renders)
                            (funcall render))))
                 (funcall callback
                          (and (eq case 'marked)
                               '(("account_id" :ref-table "accounts"
                                  :ref-column "id")))))
               (with-current-buffer buf
                 (should (= renders (if (eq case 'marked) 1 0)))
                 (let ((line (clutch-test--rendered-line-at 0)))
                   (should (eq (get-text-property (string-match "7" line)
                                                  'face line)
                               (and (eq case 'marked) 'clutch-fk-face)))))))
         (when (buffer-live-p buf)
           (kill-buffer buf)))))))

(ert-deftest clutch-test-column-info-string-contract ()
  "Column info strings should format detail text, faces, and missing metadata."
  (with-temp-buffer
    (setq-local clutch--result-columns '("id" "name"))
    (setq-local clutch--result-column-details
                (list (list :name "id" :type "INT" :nullable nil
                            :default "42" :comment "Primary key")))
    (let* ((info (clutch--column-info-string 0))
           (one-line (clutch--column-info-message-string info))
           (name-pos (string-match-p "\\bid\\b" one-line))
           (type-pos (string-match-p "INT" one-line))
           (sep-pos (string-match-p "  •  " one-line)))
      (should (string-match-p "Type: INT" info))
      (should (string-match-p "Nullable: NO" info))
      (should (string-match-p "Default: 42" info))
      (should (string-match-p "Primary key" info))
      (should name-pos)
      (should type-pos)
      (should sep-pos)
      (should (eq (get-text-property name-pos 'face one-line)
                  'clutch-field-name-face))
      (should (eq (get-text-property type-pos 'face one-line)
                  'font-lock-type-face))
      (should (eq (get-text-property sep-pos 'face one-line)
                  'font-lock-comment-face)))
    (setq-local clutch--result-column-details
                (list nil
                      (list :name "name" :type "VARCHAR(255)" :nullable t
                            :default "unnamed")))
    (let ((info (clutch--column-info-string 1)))
      (should (string-match-p "Type: VARCHAR(255)" info))
      (should (string-match-p "Nullable: YES" info))
      (should (string-match-p "Default: unnamed" info)))
    (setq-local clutch--result-column-details nil)
    (should-not (clutch--column-info-string 0))))

(ert-deftest clutch-test-result-column-details-contract ()
  "Detail resolution should map result columns by name and skip missing tables."
  (cl-letf (((symbol-function 'clutch--cached-column-details)
             (lambda (_conn _table)
               (list (list :name "ID" :type "INT" :nullable nil)
                     (list :name "NAME" :type "VARCHAR" :nullable t)))))
    (let ((result (clutch--result-column-details
                   'dummy-conn "users" '("id" "name"))))
      (should (= (length result) 2))
      (should (equal (plist-get (nth 0 result) :type) "INT"))
      (should (equal (plist-get (nth 1 result) :type) "VARCHAR"))))
  (should-not (clutch--result-column-details
               'dummy nil '("col1"))))

;;;; Edit — cell editing

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

(defmacro clutch-test--with-open-edit-cell
    (edit-var result-var result-spec cell table details &rest body)
  "Open EDIT-VAR from RESULT-VAR initialized by RESULT-SPEC, then run BODY."
  (declare (indent 6))
  (let ((spec result-spec))
    (unless (memq :connection-params spec)
      (setq spec (append spec '(:connection-params '(:backend mysql)))))
    `(clutch-test--with-result-state-buffer ,result-var ,spec
       (let ((,edit-var (clutch-test--open-edit-cell
                         ,result-var ,cell ,table ,details)))
         (unwind-protect
             (progn ,@body)
           (when (buffer-live-p ,edit-var)
             (kill-buffer ,edit-var)))))))

(defmacro clutch-test--with-auto-json-edit-cell
    (json-var parent-var result-var &rest body)
  "Open an auto JSON edit cell and run BODY with buffers bound."
  (declare (indent 3))
  `(let (,parent-var)
     (unwind-protect
         (clutch-test--with-open-edit-cell ,json-var ,result-var
             (:columns '("payload")
              :column-defs '((:name "payload" :type-category text))
              :rows '(("{\"ok\":true}"))
              :row-identity
              (clutch-test--primary-row-identity "events" '("payload") '(0)))
             '(0 0 "{\"ok\":true}")
             "events"
             (list (list :name "payload" :type "text"))
           (with-current-buffer ,json-var
             (setq ,parent-var clutch-result-edit-json--parent-buffer)
             (should clutch-result-edit-json--whole-edit-p))
           ,@body)
       (when (buffer-live-p ,parent-var)
         (kill-buffer ,parent-var)))))

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

(defmacro clutch-test--with-pop-to-buffer-capture (var &rest body)
  "Bind VAR to the buffer passed to `pop-to-buffer' while running BODY."
  (declare (indent 1) (debug (symbolp body)))
  `(let (,var)
     (unwind-protect
         (cl-letf (((symbol-function 'pop-to-buffer)
                    (lambda (buf &rest _args)
                      (setq ,var buf)
                      buf)))
           ,@body)
       (when (and ,var (buffer-live-p ,var))
         (kill-buffer ,var)))))

(defmacro clutch-test--with-insert-result-buffer (var spec &rest body)
  "Bind VAR to a result buffer initialized for insert tests by SPEC."
  (declare (indent 2))
  (let ((normalized-spec spec))
    (unless (memq :connection normalized-spec)
      (setq normalized-spec (append normalized-spec '(:connection nil))))
    (unless (memq :rows normalized-spec)
      (setq normalized-spec (append normalized-spec '(:rows nil))))
    `(clutch-test--with-result-state-buffer ,var ,normalized-spec
       ,@body)))

(ert-deftest clutch-test-edit-pending-insert-reopens-prefilled-insert-buffer ()
  "Editing a ghost insert row should reopen the staged insert with its values."
  (clutch-test--with-pop-to-buffer-capture insert-buf
    (clutch-test--with-result-state-buffer result-buf
        (:connection nil
         :connection-params '(:backend mysql)
         :source-table "shipping_incidents"
         :columns '("id" "severity" "owner")
         :rows '((1 "low" "alice"))
         :pending-inserts '((("severity" . "high") ("owner" . "bob"))))
      (cl-letf (((symbol-function 'clutch--cell-at-point)
                 (lambda () (list 1 1 "high"))))
        (with-current-buffer result-buf
          (clutch-result-edit-cell))
        (with-current-buffer insert-buf
          (should (equal clutch-result-insert--pending-index 0))
          (should (equal clutch-result-insert--table "shipping_incidents"))
          (should (string-match-p "^severity[ ]*: high$" (buffer-string)))
          (should (string-match-p "^owner[ ]*: bob$" (buffer-string))))))))

(ert-deftest clutch-test-edit-cell-shows-metadata-and-completion-hints ()
  "Edit buffer should expose enum metadata and completion affordances."
  (clutch-test--with-open-edit-cell buf result-buf
      (:connection (make-clutch-jdbc-conn :params '(:driver oracle))
       :columns '("severity")
       :column-defs '((:name "severity" :type-category text))
       :rows '(("low"))
       :row-identity
       (clutch-test--primary-row-identity "shipping_incidents" '("severity") '(0)))
      '(0 0 "low")
      "shipping_incidents"
      (list (list :name "severity" :type "enum('low','medium','high')"
                  :nullable t :default "'low'"))
    (with-current-buffer buf
      (should (string-match-p "\\[enum\\]" (format "%s" header-line-format)))

      (should (string-match-p "Set NULL.*Set DEFAULT"
                              (format "%s" header-line-format)))
      (should-not (string-match-p "Editing row" (format "%s" header-line-format)))
      (pcase-let ((`(,beg ,end ,candidates . ,_)
                   (clutch-result-edit-completion-at-point)))
        (should (= beg (point-min)))
        (should (= end (point-max)))
        (should (equal candidates '("low" "medium" "high")))))))

(ert-deftest clutch-test-jdbc-blob-encoding-survives-complete-edit-path ()
  "JDBC BLOB encoding should survive normalization, editing, staging, and wire."
  (let* ((modified "{\"message\":\"已修改\"}")
         (source
          (car (clutch-jdbc--normalize-row
                '((:__type "blob" :length 18
                   :text "{\"message\":\"原值\"}" :encoding "GB18030")))))
         parent-buf)
    (unwind-protect
        (clutch-test--with-open-edit-cell json-buf result-buf
            (:connection (make-clutch-jdbc-conn :params '(:driver oracle))
             :columns '("ID" "CONTENT")
             :column-defs '((:name "ID" :type-category numeric)
                            (:name "CONTENT" :type-category blob
                             :backend-type "BLOB"))
             :rows (list (list 1 source))
             :row-identity
             (clutch-test--primary-row-identity "DOCUMENTS" '("ID") '(0)))
            (list 0 1 source)
            "DOCUMENTS"
            (list (list :name "CONTENT" :type "BLOB"
                        :backend-type "BLOB"))
          (with-current-buffer json-buf
            (setq parent-buf clutch-result-edit-json--parent-buffer)
            (should clutch-result-edit-json--whole-edit-p)
            (erase-buffer)
            (insert modified))
          (with-current-buffer parent-buf
            (should (equal clutch-result-edit--blob-encoding "GB18030")))
          (cl-letf (((symbol-function 'quit-window)
                     (lambda (&optional kill _window)
                       (when kill
                         (kill-buffer (current-buffer))))))
            (with-current-buffer json-buf
              (clutch-result-edit-json-finish)))
          (let* ((staged (with-current-buffer result-buf
                           (cdar clutch--pending-edits)))
                 (wire
                  (clutch-jdbc--wire-param
                   (clutch-db-typed-param staged "BLOB"))))
            (should (equal staged modified))
            (should
             (equal
              (base64-decode-string (alist-get 'base64 wire))
              (encode-coding-string
               modified (coding-system-from-name "GB18030"))))))
      (when (buffer-live-p parent-buf)
        (kill-buffer parent-buf)))))

(ert-deftest clutch-test-edit-cell-opens-null-state-placeholder ()
  "Editing a NULL cell should show a placeholder while keeping buffer text empty."
  (clutch-test--with-open-edit-cell buf result-buf
      (:columns '("id" "note")
       :column-defs '((:name "id" :type-category numeric)
                      (:name "note" :type-category text))
       :rows '((1 nil))
       :row-identity
       (clutch-test--primary-row-identity "shipping_incidents" '("id") '(0)))
      '(0 1 nil)
      "shipping_incidents"
      (list (list :name "note" :type "text"))
    (with-current-buffer buf
      (should (eq clutch-result-edit--special-value 'null))
      (should (equal (buffer-string) ""))
      (should (overlayp clutch-result-edit--special-placeholder-overlay))
      (should (equal
               (substring-no-properties
                (overlay-get clutch-result-edit--special-placeholder-overlay
                             'after-string))
               "<null>"))
      (should (eq (get-text-property
                   0 'face
                   (overlay-get clutch-result-edit--special-placeholder-overlay
                                'after-string))
                  'clutch-null-face))
      (insert "hello")
      (should-not clutch-result-edit--special-value)
      (should-not (overlayp clutch-result-edit--special-placeholder-overlay))
      (should (equal (buffer-string) "hello")))))

(ert-deftest clutch-test-edit-special-value-hints-follow-column-detail ()
  "Edit hints and commands should reject unsupported column special values."
  (clutch-test--with-result-edit-buffer _edit-buf "keep"
    (setq-local clutch-result-edit--column-name "status"
                clutch-result-edit--default-supported-p t)
    (dolist (case '((nil nil nil nil)
                    (t nil t nil)
                    (nil "'new'" nil t)
                    (t "'new'" t t)))
      (pcase-let ((`(,nullable ,default ,show-null ,show-default) case))
        (setq-local clutch-result-edit--column-detail
                    (list :name "status" :nullable nullable :default default))
        (let ((header (clutch-result-edit--header-line)))
          (should (equal (list (and (string-match-p "Set NULL" header) t)
                               (and (string-match-p "Set DEFAULT" header) t))
                         (list show-null show-default))))))
    (setq-local clutch-result-edit--column-detail
                '(:name "status" :nullable nil))
    (should-error (clutch-result-edit-set-null) :type 'user-error)
    (should-error (clutch-result-edit-set-default) :type 'user-error)
    (should (equal (buffer-string) "keep"))))

(ert-deftest clutch-test-edit-cell-original-value-does-not-stage ()
  "Submitting or restoring the effective value should preserve staged state."
  (dolist (case `((unchanged 42 nil nil "42" nil nil)
                  (reverted "43" ((([1] . 1) . "43")) "42" "43" nil nil)
                  (default-unchanged
                   ,clutch--cell-default-placeholder
                   ((([1] . 1) . ,clutch--cell-default-placeholder))
                   nil "" default
                   ((([1] . 1) . ,clutch--cell-default-placeholder)))
                  (default-reverted
                   ,clutch--cell-default-placeholder
                   ((([1] . 1) . ,clutch--cell-default-placeholder))
                   "42" "" default nil)))
    (pcase-let ((`(,label ,opened-value ,pending-edits ,replacement
                          ,initial-text ,special-value ,expected-pending)
                 case))
      (ert-info ((format "case: %s" label))
        (clutch-test--with-open-edit-cell buf result-buf
            (:connection (make-clutch-jdbc-conn :params '(:driver oracle))
             :columns '("id" "qty")
             :column-defs '((:name "id" :type-category numeric)
                            (:name "qty" :type-category numeric))
             :rows '((1 42))
             :row-identity
             (clutch-test--primary-row-identity "orders" '("id") '(0))
             :pending-edits pending-edits)
            (list 0 1 opened-value)
            "orders"
            (list (list :name "qty" :type "int" :default "0"))
          (with-current-buffer buf
            (should (equal (buffer-string) initial-text))
            (should (eq clutch-result-edit--special-value special-value))
            (when special-value
              (should (equal
                       (substring-no-properties
                        (overlay-get clutch-result-edit--special-placeholder-overlay
                                     'after-string))
                       "<default>")))
            (when replacement
              (erase-buffer)
              (insert replacement))
            (cl-letf (((symbol-function 'clutch--replace-row-at-index) #'ignore)
                      ((symbol-function 'clutch--refresh-footer-line) #'ignore)
                      ((symbol-function 'quit-window) #'ignore)
                      ((symbol-function 'message) #'ignore))
              (clutch-result-edit-finish))
            (should (equal (with-current-buffer result-buf
                             clutch--pending-edits)
                           expected-pending))))))))

(ert-deftest clutch-test-edit-cell-rejects-stale-source ()
  "Finishing an edit should not stage over a changed visible row or cell."
  (dolist (case '((row-identity
                   ((1 "alice") (2 "bob"))
                   ((2 "bob") (1 "alice")))
                  (cell-value
                   ((1 "alice"))
                   ((1 "remote")))))
    (pcase-let ((`(,label ,initial-rows ,updated-rows) case))
      (ert-info ((format "case: %s" label))
        (clutch-test--with-open-edit-cell buf result-buf
            (:columns '("id" "name")
             :column-defs '((:name "id" :type-category numeric)
                            (:name "name" :type-category text))
             :rows initial-rows
             :row-identity
             (clutch-test--primary-row-identity "users" '("id") '(0))
             :pending-edits nil)
            '(0 1 "alice")
            "users"
            (list (list :name "name" :type "text"))
          (with-current-buffer result-buf
            (setq-local clutch--result-rows updated-rows))
          (with-current-buffer buf
            (erase-buffer)
            (insert "ann")
            (cl-letf (((symbol-function 'clutch--replace-row-at-index)
                       #'ignore)
                      ((symbol-function 'clutch--refresh-footer-line) #'ignore)
                      ((symbol-function 'quit-window) #'ignore))
              (let ((err (should-error (clutch-result-edit-finish)
                                       :type 'user-error)))
                (should (string-match-p "Edited row changed"
                                        (error-message-string err))))))
          (should-not (with-current-buffer result-buf
                        clutch--pending-edits)))))))

(ert-deftest clutch-test-edit-cell-entry-errors ()
  "Edit entry should fail early when row identity metadata is unavailable."
  (dolist (case
           '((no-identity
              nil nil
              "Cannot edit cell: no primary, unique, or row locator identity available for table users")
             (metadata-error
              error "metadata failed"
              "Cannot edit cell: row identity metadata failed for table users: metadata failed")
             (table-history
              nil nil
              "Cannot edit cell: the query reads table users as of another time"
              "SELECT * FROM users FOR SYSTEM_TIME ALL")))
    (pcase-let ((`(,label ,status ,message ,expected ,query) case))
      (ert-info ((format "case: %s" label))
        (clutch-test--with-result-state-buffer result-buf
            (:connection-params '(:backend mysql)
             :source-table "users"
             :last-query (or query "SELECT * FROM users")
             :columns '("id" "name")
             :column-defs '((:name "id" :type-category numeric)
                            (:name "name" :type-category text))
             :rows '((1 "alice"))
             :row-identity-status status
             :row-identity-error-message message)
          (cl-letf (((symbol-function 'clutch--cell-at-point)
                     (lambda () '(0 1 "alice"))))
            (with-current-buffer result-buf
              (let ((err (should-error (clutch-result-edit-cell)
                                       :type 'user-error)))
                (should (string-match-p (regexp-quote expected)
                                        (error-message-string err)))
                (when (eq status 'error)
                  (should (string-match-p
                           (regexp-quote clutch-debug-buffer-name)
                           (error-message-string err)))))
              (should-not (get-buffer "*clutch-edit: [0].name*")))))))))

(ert-deftest clutch-test-edit-cell-json-sub-editor-contract ()
  "JSON cells should open the JSON sub-editor with serialized JSON text."
  (let ((object (make-hash-table :test 'equal)))
    (puthash "test" t object)
    (puthash "data" (vector 1 2) object)
    (dolist (case (list (list :label "raw json"
                              :payload "{\"a\":1}"
                              :expected "{\n  \"a\": 1\n}"
                              :buffer-match "\\*clutch-edit-json: payload\\*"
                              :header-match "JSON field payload")
                        (list :label "parsed object"
                              :payload object
                              :matches '("\"test\": true" "\"data\": \\[")
                              :not-matches '("#s(hash-table"))
                        (list :label "json string"
                              :payload "hello"
                              :expected "\"hello\"")))
      (ert-info ((format "case: %s" (plist-get case :label)))
        (let ((payload (plist-get case :payload)))
          (clutch-test--with-open-edit-cell buf result-buf
              (:columns '("payload")
               :column-defs '((:name "payload" :type-category json))
               :rows (list (list payload))
               :row-identity
               (clutch-test--primary-row-identity
                "shipping_incidents" '("payload") '(0)))
              (list 0 0 payload)
              "shipping_incidents"
              (list (list :name "payload" :type "json"))
            (when-let* ((buffer-match (plist-get case :buffer-match)))
              (should (string-match-p buffer-match (buffer-name buf))))
            (with-current-buffer buf
              (should (equal clutch-result-edit-json--field-name "payload"))
              (when-let* ((header-match (plist-get case :header-match)))
                (should (string-match-p header-match
                                        (format "%s" header-line-format))))
              (let ((text (buffer-substring-no-properties
                           (point-min) (point-max))))
                (when-let* ((expected (plist-get case :expected)))
                  (should (equal text expected)))
                (dolist (pattern (plist-get case :matches))
                  (should (string-match-p pattern text)))
                (dolist (pattern (plist-get case :not-matches))
                  (should-not (string-match-p pattern text)))))))))))

(ert-deftest clutch-test-edit-cell-json-looking-text-opens-json-sub-editor ()
  "Text cells containing JSON objects should still use the JSON editor."
  (clutch-test--with-open-edit-cell buf result-buf
      (:columns '("payload")
       :column-defs '((:name "payload" :type-category text))
       :rows '(("{\"order\":{\"id\":42},\"lines\":[1,2]}"))
       :row-identity
       (clutch-test--primary-row-identity "events" '("payload") '(0)))
      '(0 0 "{\"order\":{\"id\":42},\"lines\":[1,2]}")
      "events"
      (list (list :name "payload" :type "text"))
    (should (string-match-p "\\*clutch-edit-json: payload\\*"
                            (buffer-name buf)))
    (with-current-buffer buf
      (should (string-match-p "JSON field payload"
                              (format "%s" header-line-format)))
      (should (equal (buffer-substring-no-properties (point-min) (point-max))
                     "{\n  \"order\": {\n    \"id\": 42\n  },\n  \"lines\": [\n    1,\n    2\n  ]\n}")))))

(ert-deftest clutch-test-edit-cell-auto-json-closes-edit-flow ()
  "Auto-opened JSON editors should close the parent edit flow."
  (dolist (case '(cancel finish))
    (ert-info ((format "case: %s" case))
      (clutch-test--with-auto-json-edit-cell json-buf parent-buf result-buf
        (when (eq case 'finish)
          (with-current-buffer json-buf
            (erase-buffer)
            (insert "{\"ok\":false}")))
        (cl-letf (((symbol-function 'quit-window)
                   (lambda (&optional kill _window)
                     (when kill
                       (kill-buffer (current-buffer)))))
                  ((symbol-function 'clutch--ensure-column-details)
                   (lambda (_conn _table &optional _strict)
                     (list (list :name "payload" :type "text")))))
          (with-current-buffer json-buf
            (pcase case
              ('cancel (clutch-result-edit-json-cancel))
              ('finish (clutch-result-edit-json-finish)))))
        (should-not (buffer-live-p json-buf))
        (should-not (buffer-live-p parent-buf))
        (with-current-buffer result-buf
          (should-not clutch--active-edit-cell)
          (if (eq case 'finish)
              (should (equal clutch--pending-edits
                             (list
                              (cons (cons (vector "{\"ok\":true}") 0)
                                    "{\"ok\":false}"))))
            (should-not clutch--pending-edits)))))))

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

(ert-deftest clutch-test-insert-buffer-navigation ()
  "Insert buffer TAB and RET navigation should jump between field values."
  (clutch-test--with-pop-to-buffer-capture insert-buf
    (clutch-test--with-insert-result-buffer result-buf
        (:columns '("id" "name" "created_at")
         :column-defs '((:name "id") (:name "name") (:name "created_at"))
         :source-table "users")
      (clutch-result-insert--open-buffer
       "users" result-buf '(("name" . "alice")))
      (with-current-buffer insert-buf
        (clutch-test--goto-insert-field-value "id" t)
        (clutch-result-insert-next-field)
        (should (equal (clutch-result-insert--current-field-name) "name"))
        (should (= (point) (line-end-position)))
        (clutch-result-insert-next-field)
        (should (string-prefix-p "created_at" (thing-at-point 'line t)))
        (clutch-result-insert-prev-field)
        (should (equal (clutch-result-insert--current-field-name) "name"))
        (clutch-test--goto-insert-field-value "id")
        (call-interactively (key-binding (kbd "RET")))
        (should (equal (clutch-result-insert--current-field-name) "name"))))))

(ert-deftest clutch-test-pending-insert-render-contract ()
  "Staged insert rows should show insert markers and metadata placeholders."
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
                 (lambda (&rest _)
                   (error "render should not synchronously load column details")))
                ((symbol-function 'clutch--cached-column-details)
                 (lambda (_conn _table)
                   (list (list :name "id" :generated t)
                         (list :name "name")
                         (list :name "created_at" :default "CURRENT_TIMESTAMP")
                         (list :name "notes")))))
        (setq render-state (clutch--build-render-state))
        (clutch--insert-pending-insert-rows '(0 1 2 3) [12 12 12 12] 3 0 row-positions
                                            render-state)
        (let ((rendered (buffer-string)))
          (should (string-prefix-p "│I I1 " rendered))
          (should (string-match-p "<generated>" rendered))
          (should (string-match-p "<default>" rendered))
          (should (string-match-p "alice" rendered)))))))

(ert-deftest clutch-test-pending-insert-placeholders-skip-metadata-without-inserts ()
  "Result rendering should not request insert placeholder metadata without inserts."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn
                clutch--result-columns '("id" "name")
                clutch--result-source-table "users"
                clutch--pending-inserts nil)
    (cl-letf (((symbol-function 'clutch--cached-column-details)
               (lambda (&rest _)
                 (error "column details cache should not be consulted")))
              ((symbol-function 'clutch--ensure-column-details-async)
               (lambda (&rest _)
                 (error "column details should not be queued"))))
      (should-not (plist-get (clutch--build-render-state)
                             :insert-placeholders)))))

(ert-deftest clutch-test-pending-insert-placeholders-queue-metadata-and-render-empty ()
  "Staged insert rendering should keep column shape while metadata loads."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn
                clutch--result-columns '("id" "name")
                clutch--result-source-table "users"
                clutch--pending-inserts '((("name" . "alice"))))
    (let (queued)
      (cl-letf (((symbol-function 'clutch--cached-column-details)
                 (lambda (&rest _) nil))
                ((symbol-function 'clutch--ensure-column-details-async)
                 (lambda (_conn table)
                   (setq queued table))))
        (let ((render-state (clutch--build-render-state)))
          (should (equal queued "users"))
          (should (equal (plist-get render-state :insert-placeholders)
                         '(nil nil)))
          (should (equal (clutch--pending-insert-render-rows render-state)
                         '((nil "alice")))))))))

(ert-deftest clutch-test-insert-fill-current-time-respects-column-type ()
  "The insert buffer time-filling helper should use result column metadata."
  (clutch-test--with-insert-result-buffer result-buf
      (:columns '("due_on" "created_at" "name")
       :column-defs '((:name "due_on" :type-category date)
                      (:name "created_at" :type-category datetime)
                      (:name "name" :type-category text)))
    (clutch-test--with-pop-to-buffer-capture insert-buf
      (clutch-result-insert--open-buffer
       "users" result-buf
       '(("due_on" . "2024-01-01")
         ("created_at" . "2024-01-01 00:00:00")
         ("name" . "alice")))
      (with-current-buffer insert-buf
      (cl-letf (((symbol-function 'current-time)
                 (lambda () (encode-time 30 45 13 12 3 2026))))
        (goto-char (point-min))
        (clutch-result-insert-fill-current-time)
        (should (equal
                 (plist-get (clutch-result-insert--field-state "due_on") :value)
                 "2026-03-12"))
        (forward-line 1)
        (clutch-result-insert-fill-current-time)
        (should (equal
                 (plist-get (clutch-result-insert--field-state "created_at") :value)
                 "2026-03-12 13:45:30"))
        (forward-line 1)
        (should-error (clutch-result-insert-fill-current-time)
                      :type 'user-error))))))

(ert-deftest clutch-test-insert-buffer-labels-show_field_metadata ()
  "Insert buffer labels should show field metadata without changing parsed names."
  (clutch-test--with-pop-to-buffer-capture insert-buf
    (clutch-test--with-insert-result-buffer result-buf
        (:columns '("id" "severity" "postmortem" "is_ship_blocked" "opened_at")
         :column-defs '((:name "id" :type-category numeric)
                        (:name "severity" :type-category text)
                        (:name "postmortem" :type-category json)
                        (:name "is_ship_blocked" :type-category numeric)
                        (:name "opened_at" :type-category datetime))
         :connection 'fake-conn)
      (cl-letf (((symbol-function 'clutch--ensure-column-details)
                 (lambda (_conn _table)
                   (list (list :name "id" :type "int" :generated t :nullable nil)
                         (list :name "severity" :type "enum('low','medium')" :nullable nil)
                         (list :name "postmortem" :type "json" :nullable t)
                         (list :name "is_ship_blocked" :type "tinyint(1)" :default "0" :nullable nil)
                         (list :name "opened_at" :type "datetime" :nullable nil)))))
        (clutch-result-insert--open-buffer "shipping_incidents" result-buf))
      (with-current-buffer insert-buf
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
        (clutch-test--goto-insert-field-value "severity" t)
        (insert "low")
        (goto-char (point-min))
        (let ((fields (clutch-result-insert--parse-fields)))
          (should (equal fields '(("severity" . "low")))))))))

(ert-deftest clutch-test-insert-buffer-shows-all-fields-by-default ()
  :tags '(:smoke)
  "Insert buffers should render every field without a sparse toggle."
  (clutch-test--with-pop-to-buffer-capture insert-buf
    (clutch-test--with-insert-result-buffer result-buf
        (:columns '("id" "severity" "owner" "created_at")
         :column-defs '((:name "id" :type-category numeric)
                        (:name "severity" :type-category text)
                        (:name "owner" :type-category text)
                        (:name "created_at" :type-category datetime))
         :connection 'fake-conn
         :source-table "shipping_incidents")
      (cl-letf (((symbol-function 'clutch--ensure-column-details)
                 (lambda (_conn _table)
                   (list (list :name "id" :type "int" :generated t :nullable nil)
                         (list :name "severity" :type "enum('low','medium','high')" :nullable nil)
                         (list :name "owner" :type "varchar(64)" :default "system" :nullable t)
                         (list :name "created_at" :type "datetime"
                               :default "CURRENT_TIMESTAMP" :nullable t)))))
        (clutch-result-insert--open-buffer "shipping_incidents" result-buf))
      (with-current-buffer insert-buf
        (should (string-match-p "C-c \\. Set current time"
                                (substring-no-properties
                                 (clutch-result-insert--header-line))))
        (should (string-match-p "^id[ ]+\\[generated\\]: $" (buffer-string)))
        (should (string-match-p "^severity[ ]+\\[enum required\\]: $" (buffer-string)))
        (should (string-match-p "^owner[ ]+\\[default=system\\]: $" (buffer-string)))
        (should (string-match-p "^created_at[ ]+\\[default=CURRENT_TIMESTAMP datetime\\]: "
                                (buffer-string)))
        (should-not (lookup-key clutch--result-insert-major-mode-map
                                (kbd "C-c C-a")))
        (goto-char (point-min))
        (re-search-forward "^owner.*: " nil t)
        (insert "bob")
        (should (string-match-p "^owner[ ]+\\[default=system\\]: bob$"
                                (buffer-string)))))))

(ert-deftest clutch-test-clone-row-to-insert-prefills-effective-result-values ()
  "Cloning a result row should reuse visible row values and staged edits."
  (clutch-test--with-pop-to-buffer-capture insert-buf
    (clutch-test--with-result-state-buffer result-buf
        (:connection-params '(:backend mysql)
         :last-query "SELECT * FROM shipping_incidents"
         :source-table "shipping_incidents"
         :columns '("id" "severity" "owner" "created_at")
         :column-defs '((:name "id" :type-category numeric)
                        (:name "severity" :type-category text)
                        (:name "owner" :type-category text)
                        (:name "created_at" :type-category datetime))
         :rows '((1 "low" "alice" "2026-03-01 10:00:00"))
         :row-identity (clutch-test--primary-row-identity
                        "shipping_incidents" '("id") '(0))
         :pending-edits `((([1] . 1) . "high")
                          (([1] . 3) . ,clutch--cell-default-placeholder)))
      (cl-letf (((symbol-function 'clutch--row-idx-at-line)
                 (lambda () 0))
                ((symbol-function 'clutch--ensure-column-details)
                 (lambda (_conn _table)
                   (list (list :name "id" :type "int" :generated t :nullable nil)
                         (list :name "severity" :type "enum('low','medium','high')" :nullable nil)
                         (list :name "owner" :type "varchar(64)" :nullable t)
                         (list :name "created_at" :type "datetime"
                               :default "CURRENT_TIMESTAMP" :nullable t)))))
        (with-current-buffer result-buf
          (clutch-clone-row-to-insert)))
      (with-current-buffer insert-buf
        (should (string-match-p "C-c \\. Set current time"
                                (substring-no-properties
                                 (clutch-result-insert--header-line))))
        (should (string-match-p "^id[ ]+\\[generated\\]: $" (buffer-string)))
        (should (string-match-p "^severity[ ]+\\[enum required\\]: high$"
                                (buffer-string)))
        (should (string-match-p "^owner[ ]*: alice$" (buffer-string)))
        (should (string-match-p "^created_at .*: $" (buffer-string)))
        (should-not (string-match-p "2026-03-01 10:00:00"
                                    (buffer-string)))))))

(ert-deftest clutch-test-clone-row-to-insert-from-record-buffer ()
  "Cloning from a record buffer should prefill the current visible record row."
  (dolist (case '((unfiltered ((7 "carol")) nil nil "carol" nil)
                  (filtered ((1 "alice") (2 "bob")) "bob" ((2 "bob"))
                   "bob" "alice")))
    (pcase-let ((`(,label ,rows ,filter ,filtered-rows ,expected ,rejected)
                 case))
      (ert-info ((format "case: %s" label))
        (let ((record-buf (generate-new-buffer "*clutch-record*")))
          (unwind-protect
              (clutch-test--with-pop-to-buffer-capture insert-buf
                (clutch-test--with-result-state-buffer result-buf
                    (:connection-params '(:backend mysql)
                     :last-query "SELECT * FROM shipping_incidents"
                     :source-table "shipping_incidents"
                     :columns '("id" "owner")
                     :column-defs '((:name "id" :type-category numeric)
                                    (:name "owner" :type-category text))
                     :rows rows
                     :filter-pattern filter
                     :filtered-rows filtered-rows)
                  (with-current-buffer record-buf
                    (clutch-record-mode)
                    (setq-local clutch-record--result-buffer result-buf
                                clutch-record--row-idx 0))
                  (cl-letf (((symbol-function 'clutch--ensure-column-details)
                             (lambda (_conn _table)
                               (list (list :name "id" :type "int"
                                           :generated t :nullable nil)
                                     (list :name "owner" :type "varchar(64)"
                                           :nullable t)))))
                    (with-current-buffer record-buf
                      (clutch-clone-row-to-insert)))
                  (with-current-buffer insert-buf
                    (should (string-match-p "^id[ ]+\\[generated\\]: $"
                                            (buffer-string)))
                    (should (string-match-p
                             (format "^owner[ ]*: %s$" expected)
                             (buffer-string)))
                    (when rejected
                      (should-not (string-match-p rejected
                                                  (buffer-string)))))))
            (when (buffer-live-p record-buf)
              (kill-buffer record-buf))))))))

(ert-deftest clutch-test-clone-row-to-insert-leaves-primary-key-empty ()
  "Clone-to-insert should render primary-key fields without prefilling them."
  (clutch-test--with-pop-to-buffer-capture insert-buf
    (clutch-test--with-result-state-buffer result-buf
        (:connection-params '(:backend mysql)
         :last-query "SELECT * FROM incident_codes"
         :source-table "incident_codes"
         :columns '("id" "label")
         :column-defs '((:name "id" :type-category numeric)
                        (:name "label" :type-category text))
         :rows '((42 "duplicate me")))
      (cl-letf (((symbol-function 'clutch--row-idx-at-line)
                 (lambda () 0))
                ((symbol-function 'clutch--ensure-column-details)
                 (lambda (_conn _table)
                   (list (list :name "id" :type "int" :primary-key t :nullable nil)
                         (list :name "label" :type "varchar(64)" :nullable nil)))))
        (with-current-buffer result-buf
          (clutch-clone-row-to-insert)))
      (with-current-buffer insert-buf
        (should (string-match-p "^id[ ]+\\[required\\]: $" (buffer-string)))
        (should (string-match-p "^label[ ]+\\[required\\]: duplicate me$"
                                (buffer-string)))))))

(ert-deftest clutch-test-insert-import-delimited-preserves-clipboard-errors ()
  "Clipboard failures must not be misreported as an empty kill ring."
  (with-temp-buffer
    (insert "existing form text")
    (let ((kill-ring '("owner,severity\nbob,high\n"))
          (interprogram-paste-function
           (lambda () (error "Clipboard provider failed"))))
      (should (equal
               (should-error
                (call-interactively #'clutch-result-insert-import-delimited))
               '(error "Clipboard provider failed"))))
    (let ((kill-ring nil)
          (interprogram-paste-function nil))
      (should (string-match-p
               "Kill ring is empty"
               (error-message-string
                (should-error
                 (call-interactively #'clutch-result-insert-import-delimited))))))
    (should (equal (buffer-string) "existing form text"))))

(ert-deftest clutch-test-insert-import-delimited-parses-quoted-csv-row ()
  "Single-row CSV import should prefill the current insert form."
  (clutch-test--with-pop-to-buffer-capture insert-buf
    (clutch-test--with-insert-result-buffer result-buf
        (:columns '("owner" "severity")
         :column-defs '((:name "owner" :type-category text)
                        (:name "severity" :type-category text))
         :connection 'fake-conn
         :source-table "shipping_incidents")
      (cl-letf (((symbol-function 'clutch--ensure-column-details)
                 (lambda (_conn _table)
                   (list (list :name "owner" :type "varchar(64)" :nullable t)
                         (list :name "severity" :type "enum('low','high')" :nullable nil)))))
        (clutch-result-insert--open-buffer "shipping_incidents" result-buf))
      (with-current-buffer insert-buf
        (clutch-result-insert-import-delimited
         "owner,severity\n\"Bob, Jr.\",high\n")
        (should (string-match-p "^owner[ ]*: Bob, Jr\\.$" (buffer-string)))
        (should (string-match-p "^severity[ ]+\\[enum required\\]: high$"
                                (buffer-string)))))))

(ert-deftest clutch-test-insert-import-delimited-stages-multi-row-header-mapping ()
  "Multi-row delimited import should stage inserts by header names."
  (clutch-test--with-pop-to-buffer-capture insert-buf
    (clutch-test--with-insert-result-buffer result-buf
        (:columns '("severity" "owner" "created_at")
         :column-defs '((:name "severity" :type-category text)
                        (:name "owner" :type-category text)
                        (:name "created_at" :type-category datetime))
         :connection 'fake-conn
         :source-table "shipping_incidents")
      (cl-letf (((symbol-function 'clutch--ensure-column-details)
                 (lambda (_conn _table)
                   (list (list :name "severity" :type "enum('low','high')" :nullable nil)
                         (list :name "owner" :type "varchar(64)" :nullable t)
                         (list :name "created_at" :type "datetime"
                               :default "CURRENT_TIMESTAMP" :nullable t))))
                ((symbol-function 'clutch--refresh-display) #'ignore))
        (clutch-result-insert--open-buffer "shipping_incidents" result-buf)
        (with-current-buffer insert-buf
          (clutch-result-insert-import-delimited
           "owner\tseverity\nbob\thigh\nann\tlow\n")
          (should (equal (with-current-buffer result-buf
                           clutch--pending-inserts)
                         '((("owner" . "bob") ("severity" . "high"))
                            (("owner" . "ann") ("severity" . "low")))))
          (should (string-match-p "^severity[ ]+\\[enum required\\]: $"
                                  (buffer-string))))))))

(ert-deftest clutch-test-insert-import-refuses-while-a-query-runs ()
  "Importing rows should be refused while a statement runs, as staging is.
The page that statement brings would replace the staged rows."
  (clutch-test--with-pop-to-buffer-capture insert-buf
    (clutch-test--with-insert-result-buffer result-buf
        (:columns '("severity" "owner")
         :column-defs '((:name "severity" :type-category text)
                        (:name "owner" :type-category text))
         :connection 'fake-conn
         :source-table "shipping_incidents")
      (let ((clutch--running-queries (make-hash-table :test 'eq)))
        (cl-letf (((symbol-function 'clutch--ensure-column-details)
                   (lambda (_conn _table)
                     (list (list :name "severity" :type "text" :nullable t)
                           (list :name "owner" :type "text" :nullable t))))
                  ((symbol-function 'clutch--refresh-display) #'ignore))
          (clutch-result-insert--open-buffer "shipping_incidents" result-buf)
          (puthash 'fake-conn (list :buffer result-buf) clutch--running-queries)
          (with-current-buffer insert-buf
            (should-error (clutch-result-insert-import-delimited
                           "owner\tseverity\nbob\thigh\nann\tlow\n")
                          :type 'user-error))
          (should-not (with-current-buffer result-buf
                        clutch--pending-inserts)))))))

;;;; Edit — staged mutations (row identity)

(ert-deftest clutch-test-insert-stage-replaces-existing-pending-insert ()
  "Staging a re-edited insert should replace the pending entry in place."
  (clutch-test--with-result-state-buffer result-buf
      (:connection nil
       :pending-inserts '((("severity" . "low"))))
    (let (replaced insert-buf)
      (cl-letf (((symbol-function 'clutch--refresh-display)
                 (lambda ()
                   (error "existing insert update should use row replacement")))
                ((symbol-function 'clutch--replace-row-at-index)
                 (lambda (ridx)
                   (setq replaced ridx)))
                ((symbol-function 'quit-window) #'ignore))
        (cl-letf (((symbol-function 'pop-to-buffer)
                   (lambda (buf &rest _args) (setq insert-buf buf) buf)))
          (with-current-buffer result-buf
            (setq-local clutch--result-columns '("severity")
                        clutch--result-column-defs '((:name "severity"))
                        clutch--result-source-table "incidents"))
          (clutch-result-insert--open-buffer
           "incidents" result-buf '(("severity" . "low")) 0)
          (with-current-buffer insert-buf
            (clutch-test--set-insert-field-value "severity" "high")
            (clutch-result-insert-stage))))
      (when (buffer-live-p insert-buf) (kill-buffer insert-buf))
      (should (= replaced 2))
      (should (equal (with-current-buffer result-buf
                       clutch--pending-inserts)
                     '((("severity" . "high"))))))))

(ert-deftest clutch-test-insert-stage-appends-new-pending-insert-locally ()
  "Staging a new insert should append one ghost row without full redraw."
  (clutch-test--with-result-state-buffer result-buf
      (:connection nil
       :pending-inserts nil)
    (let (appended insert-buf)
      (cl-letf (((symbol-function 'clutch--refresh-display)
                 (lambda ()
                   (error "new insert should use row append")))
                ((symbol-function 'clutch--append-pending-insert-row)
                 (lambda (iidx)
                   (setq appended iidx)))
                ((symbol-function 'quit-window) #'ignore))
        (cl-letf (((symbol-function 'pop-to-buffer)
                   (lambda (buf &rest _args) (setq insert-buf buf) buf)))
          (with-current-buffer result-buf
            (setq-local clutch--result-columns '("name")
                        clutch--result-column-defs '((:name "name"))
                        clutch--result-source-table "users"))
          (clutch-result-insert--open-buffer "users" result-buf)
          (with-current-buffer insert-buf
            (clutch-test--set-insert-field-value "name" "carol")
            (clutch-result-insert-stage))))
      (when (buffer-live-p insert-buf) (kill-buffer insert-buf))
      (should (= appended 0))
      (should (equal (with-current-buffer result-buf
                       clutch--pending-inserts)
                     '((("name" . "carol"))))))))

(ert-deftest clutch-test-insert-stage-rejects-stale-result-table-before-closing ()
  "Insert staging should keep the form open when the parent result table changed."
  (let (closed)
    (clutch-test--with-pop-to-buffer-capture insert-buf
      (clutch-test--with-insert-result-buffer result-buf
          (:columns '("name")
           :column-defs '((:name "name" :type-category text))
           :connection 'fake-conn
           :source-table "users")
        (cl-letf (((symbol-function 'clutch--ensure-column-details)
                   (lambda (&rest _) nil)))
          (clutch-result-insert--open-buffer "users" result-buf))
        (with-current-buffer insert-buf
          (clutch-test--set-insert-field-value "name" "alice"))
        (with-current-buffer result-buf
          (setq-local clutch--result-source-table "orders"))
        (with-current-buffer insert-buf
          (cl-letf (((symbol-function 'quit-window)
                     (lambda (&rest _) (setq closed t)))
                    ((symbol-function 'clutch--refresh-display) #'ignore))
            (let ((err (should-error (clutch-result-insert-stage)
                                     :type 'user-error)))
              (should (string-match-p "Result table changed"
                                      (error-message-string err))))))
        (should-not closed)
        (should-not (with-current-buffer result-buf
                      clutch--pending-inserts))))))

(ert-deftest clutch-test-apply-edit-errors-clearly-without-row-identity ()
  "Edit staging should explain why update/delete are disabled."
  (clutch-test--with-result-state
      (:connection-params '(:backend mysql)
       :last-query "SELECT * FROM users"
       :source-table "users"
       :rows '((1 "before")))
    (let ((err (should-error (clutch-result--apply-edit
                              0 1 "after"
                              (list :identity [1]
                                    :original "before"
                                    :original-state '(nil . "before")))
                             :type 'user-error)))
      (should (string-match-p
               "no primary, unique, or row locator identity available for table users"
               (error-message-string err))))))

(ert-deftest clutch-test-apply-edit-with-filter-noop-uses-visible-row ()
  "Edit staging should compare against the filtered display row."
  (clutch-test--with-result-state
      (:columns '("id" "name")
       :last-query "SELECT id, name FROM users"
       :source-table "users"
       :rows '((1 "alpha") (2 "beta"))
       :filter-pattern "beta"
       :filtered-rows '((2 "beta"))
       :row-identity (clutch-test--primary-row-identity "users" '("id") '(0)))
    (cl-letf (((symbol-function 'clutch--replace-row-at-index) #'ignore)
              ((symbol-function 'clutch--refresh-footer-line) #'ignore)
              ((symbol-function 'message) #'ignore))
      (clutch-result--apply-edit
       0 1 "beta" (list :identity [2]
                        :original "beta"
                        :original-state '(nil . "beta")))
      (should-not clutch--pending-edits))))

(ert-deftest clutch-test-delete-rows-errors-clearly-without-row-identity ()
  "Delete staging should explain why update/delete are disabled."
  (clutch-test--with-result-state
      (:connection-params '(:backend mysql)
       :last-query "SELECT * FROM users"
       :source-table "users"
       :rows '((1 "before")))
    (cl-letf (((symbol-function 'clutch--selected-row-indices) (lambda () '(0)))
              ((symbol-function 'clutch--refresh-display) #'ignore))
      (let ((err (should-error (clutch-result-delete-rows)
                               :type 'user-error)))
        (should (string-match-p
                 "no primary, unique, or row locator identity available for table users"
                 (error-message-string err)))))))

(ert-deftest clutch-test-discard-pending-at-point-contract ()
  "Discarding at point should remove matching delete, insert, or edit state."
  (let ((identity (clutch-test--primary-row-identity "users" '("id") '(0))))
    (dolist (case
             (list
              (list :label "delete"
                    :row 0
                    :state 'clutch--pending-deletes
                    :spec (list :columns '("id" "name")
                                :rows '((42 "alice"))
                                :row-identity identity
                                :pending-deletes (list (vector 42))))
              (list :label "insert"
                    :row 1
                    :state 'clutch--pending-inserts
                    :spec (list :columns '("id" "name")
                                :rows '((1 "x"))
                                :pending-inserts
                                (list '(("id" . "99") ("name" . "new")))))
              (list :label "edit"
                    :row 0
                    :column 1
                    :state 'clutch--pending-edits
                    :spec (list :columns '("id" "name")
                                :rows '((42 "alice"))
                                :row-identity identity
                                :pending-edits
                                (list (cons (cons (vector 42) 1)
                                            "carol"))))))
      (ert-info ((format "case: %s" (plist-get case :label)))
        (with-temp-buffer
          (clutch-test--init-result-state (plist-get case :spec))
          (cl-letf (((symbol-function 'clutch--row-idx-at-line)
                     (lambda () (plist-get case :row)))
                    ((symbol-function 'clutch--col-idx-at-point)
                     (lambda () (plist-get case :column)))
                    ((symbol-function 'clutch--refresh-display) #'ignore))
            (clutch-result-discard-pending-at-point)
            (should-not (symbol-value (plist-get case :state)))))))))

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

(ert-deftest clutch-test-submit-orders-insert-update-delete ()
  "Submit executes INSERT before UPDATE before DELETE."
  (clutch-test--with-result-state
      (:columns '("id" "name")
       :rows '((1 "a") (2 "b"))
       :pending-inserts '((("id" . "3") ("name" . "c")))
       :pending-edits (list (cons (cons (vector 1) 1) "a2"))
       :pending-deletes (list (vector 2)))
    (let (atomic executed reverts)
      (setq-local revert-buffer-function
                  (lambda (ignore-auto noconfirm)
                    (push (list ignore-auto noconfirm
                                clutch--pending-edits clutch--pending-deletes
                                clutch--pending-inserts)
                          reverts)))
      (cl-letf (((symbol-function 'clutch-result--build-pending-insert-statements)
                 (lambda () '(("INSERT INTO users (id, name) VALUES (?, ?)" . ("3" "c")))))
                ((symbol-function 'clutch-result--build-update-statements)
                 (lambda () '(("UPDATE users SET name = ? WHERE id = ?" . ("a2" 1)))))
                ((symbol-function 'clutch-result--build-pending-delete-statements)
                 (lambda () '(("DELETE FROM users WHERE id = ?" . (2)))))
                ((symbol-function 'clutch-db-escape-literal)
                 (lambda (_conn value) (format "'%s'" value)))
                ((symbol-function 'yes-or-no-p) (lambda (_) t))
                ((symbol-function 'clutch-db-manual-commit-p) (lambda (_) nil))
                ((symbol-function 'clutch-db-call-with-atomic-batch)
                 (lambda (_conn function)
                   (setq atomic t)
                   (funcall function)))
                ((symbol-function 'clutch--run-db-query)
                 (lambda (_conn sql &optional params _defer-transaction-state)
                   (setq executed (append executed (list (cons sql params))))
                   (make-clutch-db-result :affected-rows 1))))
        (clutch-result-submit)
        (should atomic)
        (should (= (length executed) 3))
        (should (string-prefix-p "INSERT" (car (nth 0 executed))))
        (should (equal (cdr (nth 0 executed)) '("3" "c")))
        (should (string-prefix-p "UPDATE" (car (nth 1 executed))))
        (should (equal (cdr (nth 1 executed)) '("a2" 1)))
        (should (string-prefix-p "DELETE" (car (nth 2 executed))))
        (should (equal (cdr (nth 2 executed)) '(2)))
        (should (equal reverts '((nil t nil nil nil))))))))

(ert-deftest clutch-test-submit-validates-before-auto-commit ()
  "Auto submit should roll back when row-count validation fails."
  (clutch-test--with-result-state
      (:pending-edits '(edit))
    (let (committed rolled-back reverted)
      (setq-local revert-buffer-function
                  (lambda (&rest _args) (setq reverted t)))
      (cl-letf (((symbol-function 'clutch-result--build-update-statements)
                 (lambda ()
                   '(("UPDATE users SET name = ? WHERE id = ?" . ("x" 1)))))
                ((symbol-function 'clutch-db-escape-literal)
                 (lambda (_conn value) (format "'%s'" value)))
                ((symbol-function 'yes-or-no-p) (lambda (_) t))
                ((symbol-function 'clutch-db-manual-commit-p) (lambda (_) nil))
                ((symbol-function 'clutch--run-db-query)
                 (lambda (&rest _)
                   (make-clutch-db-result :affected-rows 2)))
                ((symbol-function 'clutch-db-call-with-atomic-batch)
                 (lambda (_conn function)
                   (condition-case err
                       (prog1 (funcall function)
                         (setq committed t))
                     ((error quit)
                      (setq rolled-back t)
                      (signal (car err) (cdr err)))))))
        (let ((err (should-error (clutch-result-submit) :type 'user-error)))
          (should (string-match-p "Mutation matched 2 rows"
                                  (error-message-string err))))
        (should rolled-back)
        (should-not committed)
        (should-not reverted)
        (should (equal clutch--pending-edits '(edit)))))))

(ert-deftest clutch-test-submit-refuses-while-a-query-runs ()
  "Submitting staged changes should be refused while a statement runs.
JDBC opens the batch with a synchronous call that waits for the running
statement, so the refusal has to come before the prompt and the batch."
  (clutch-test--with-result-state
      (:pending-edits '(edit))
    (let ((clutch--running-queries (make-hash-table :test 'eq))
          prompted batched)
      (puthash clutch-connection (list :buffer (current-buffer)) clutch--running-queries)
      (cl-letf (((symbol-function 'clutch-result--build-update-statements)
                 (lambda ()
                   '(("UPDATE users SET name = ? WHERE id = ?" . ("x" 1)))))
                ((symbol-function 'clutch-db-escape-literal)
                 (lambda (_conn value) (format "'%s'" value)))
                ((symbol-function 'yes-or-no-p) (lambda (_) (setq prompted t)))
                ((symbol-function 'clutch-db-call-with-atomic-batch)
                 (lambda (&rest _) (setq batched t))))
        (should (string-match-p
                 "A query is running"
                 (error-message-string
                  (should-error (clutch-result-submit) :type 'user-error))))
        (should-not prompted)
        (should-not batched)
        (should (equal clutch--pending-edits '(edit)))))))

(ert-deftest clutch-test-submit-manual-batch-uses-atomic-backend-boundary ()
  "Manual staged submit should be atomic without committing the user transaction."
  (let ((clutch--tx-state-cache (make-hash-table :test 'eq)))
    (clutch-test--with-result-state
        (:pending-inserts '(first second))
      (let (atomic executed notice)
      (setq-local revert-buffer-function
                  (lambda (&rest _args) nil))
      (cl-letf (((symbol-function 'clutch-result--build-pending-insert-statements)
                 (lambda () '(("INSERT first") ("INSERT second"))))
                ((symbol-function 'clutch-db-escape-literal)
                 (lambda (_conn value) (format "'%s'" value)))
                ((symbol-function 'yes-or-no-p) (lambda (_) t))
                ((symbol-function 'clutch-db-manual-commit-p) (lambda (_) t))
                ((symbol-function 'clutch--tx-dirty-p) (lambda (_) t))
                ((symbol-function 'clutch-db-call-with-atomic-batch)
                 (lambda (_conn function)
                   (setq atomic t)
                   (funcall function)))
                ((symbol-function 'clutch--run-db-query)
                 (lambda (_conn sql &optional _params _defer-transaction-state)
                   (push sql executed)
                   (make-clutch-db-result :affected-rows 1)))
                ((symbol-function 'message)
                 (lambda (format-string &rest args)
                   (setq notice (apply #'format format-string args)))))
        (clutch-result-submit)
        (should (equal (nreverse executed) '("INSERT first" "INSERT second")))
        (should atomic)
        (should (equal notice "2 changes submitted"))
        (should-not clutch--pending-inserts))))))

(ert-deftest clutch-test-submit-handles-known-session-restore-outcomes ()
  "Pending state must follow the known outcome when restoring Auto mode fails."
  (dolist (outcome '(committed rolled-back))
    (clutch-test--with-result-state
        (:pending-inserts '(first second))
      (let (refreshed reverted)
        (setq-local revert-buffer-function
                    (lambda (&rest _args) (setq reverted t)))
        (cl-letf (((symbol-function 'clutch-result--build-pending-insert-statements)
                   (lambda () '(("INSERT first") ("INSERT second"))))
                  ((symbol-function 'clutch-db-escape-literal)
                   (lambda (_conn value) (format "'%s'" value)))
                  ((symbol-function 'yes-or-no-p) (lambda (_) t))
                  ((symbol-function 'clutch--refresh-transaction-ui)
                   (lambda (_) (setq refreshed t)))
                  ((symbol-function 'clutch-db-call-with-atomic-batch)
                   (lambda (&rest _)
                     (signal
                      'clutch-db-session-restore-error
                      (list "Restoring auto-commit failed"
                            :outcome outcome)))))
          (should-error (clutch-result-submit) :type 'user-error)
          (should refreshed)
          (if (eq outcome 'committed)
              (should-not clutch--pending-inserts)
            (should (equal clutch--pending-inserts '(first second))))
          (should (eq (not (null reverted))
                      (eq outcome 'committed))))))))

(ert-deftest clutch-test-submit-marks-unknown-batch-outcome-uncertain ()
  "An uncertain batch should retain staging and require transaction recovery."
  (let ((clutch--tx-state-cache (make-hash-table :test 'eq)))
    (clutch-test--with-result-state
        (:pending-inserts '(first second))
      (let (reverted)
        (setq-local revert-buffer-function
                    (lambda (&rest _args) (setq reverted t)))
        (cl-letf (((symbol-function 'clutch-result--build-pending-insert-statements)
                   (lambda () '(("INSERT first") ("INSERT second"))))
                  ((symbol-function 'clutch-db-escape-literal)
                   (lambda (_conn value) (format "'%s'" value)))
                  ((symbol-function 'yes-or-no-p) (lambda (_) t))
                  ((symbol-function 'clutch-db-manual-commit-supported-p)
                   (lambda (_) t))
                  ((symbol-function 'clutch-db-manual-commit-p) (lambda (_) t))
                  ((symbol-function 'clutch-db-call-with-atomic-batch)
                   (lambda (_conn function)
                     (funcall function)
                     (signal
                      'clutch-db-batch-outcome-uncertain
                      '("commit outcome is uncertain"
                        :phase commit))))
                  ((symbol-function 'clutch--run-db-query)
                   (lambda (&rest _)
                     (make-clutch-db-result :affected-rows 1)))
                  ((symbol-function 'clutch--refresh-transaction-ui) #'ignore))
          (let ((err (should-error (clutch-result-submit) :type 'user-error)))
            (should (string-match-p
                     "roll back or reconnect"
                     (error-message-string err))))
          (should (clutch--tx-uncertain-p clutch-connection))
          (should (equal clutch--pending-inserts '(first second)))
          (should-not reverted))))))

(ert-deftest clutch-test-rollback-to-a-savepoint-keeps-the-transaction-dirty ()
  "Only a rollback of the whole transaction should clear its uncommitted work.
A rollback to a savepoint keeps the work done before the savepoint, which a
disconnect then lost without asking.  SQL Server's ROLLBACK TRANSACTION with
a name may name a savepoint, so it keeps the work too, also when the name is
a word that ends a whole rollback, such as CHAIN."
  (pcase-dolist (`(,sql ,state)
                 '(("ROLLBACK TO SAVEPOINT s" dirty)
                   ("rollback to s;" dirty)
                   ("ROLLBACK WORK TO SAVEPOINT s" dirty)
                   ("ROLLBACK TRANSACTION TO SAVEPOINT s" dirty)
                   ("ROLLBACK TRAN s" dirty)
                   ("ROLLBACK TRANSACTION chain" dirty)
                   ("ROLLBACK TRAN release" dirty)
                   ("ROLLBACK TRAN no" dirty)
                   ("ROLLBACK" nil)
                   ("rollback work;" nil)
                   ("ROLLBACK TRANSACTION" nil)
                   ("ROLLBACK TRANSACTION AND CHAIN" nil)
                   ("ROLLBACK AND NO CHAIN" nil)
                   ("ROLLBACK WORK AND CHAIN NO RELEASE" nil)
                   ("-- done\nROLLBACK" nil)
                   ("COMMIT" nil)))
    (ert-info (sql)
      (let ((clutch--tx-state-cache (make-hash-table :test 'eq))
            (clutch--running-queries (make-hash-table :test 'eq)))
        (cl-letf (((symbol-function 'clutch-db-manual-commit-p) (lambda (_) t))
                  ((symbol-function 'clutch-db-query)
                   (lambda (&rest _) (make-clutch-db-result :affected-rows 1)))
                  ((symbol-function 'clutch--clear-connection-problem-capture)
                   #'ignore))
          (dolist (statement (list "UPDATE t SET n = 1" "SAVEPOINT s"
                                   "UPDATE t SET n = 2" sql))
            (clutch--run-db-query 'tx-conn statement))
          (should (eq (clutch--tx-state 'tx-conn) state)))))))

;;;; Edit — validation

(ert-deftest clutch-test-insert-local-validation-updates-inline-error ()
  "Insert field live validation should show and clear inline errors."
  (clutch-test--with-insert-result-buffer result-buf
      (:columns '("impact_score")
       :column-defs '((:name "impact_score" :type-category numeric))
       :connection 'fake-conn)
    (clutch-test--with-pop-to-buffer-capture insert-buf
      (cl-letf (((symbol-function 'clutch--ensure-column-details)
                 (lambda (_conn _table)
                   (list (list :name "impact_score" :type "decimal(5,1)")))))
        (clutch-result-insert--open-buffer "shipping_incidents" result-buf)
        (with-current-buffer insert-buf
        (clutch-test--goto-insert-field-value "impact_score")
        (insert "x")
        (clutch-result-insert--run-idle-validation insert-buf)
        (let* ((field (clutch-result-insert--field-state "impact_score"))
               (after (overlay-get (plist-get field :error-overlay)
                                   'after-string)))
          (should (equal (plist-get field :error-message)
                         "Field impact_score expects a numeric value"))
          (should (overlayp (plist-get field :error-overlay)))
          (should (string-match-p "\\[invalid numeric\\]" after))
          (should-not (string-prefix-p "\n" after))
          (clutch-test--goto-insert-field-value "impact_score")
          (delete-region (point) (line-end-position))
          (insert "1.5")
          (clutch-result-insert--run-idle-validation insert-buf)
          (setq field (clutch-result-insert--field-state "impact_score"))
          (should-not (plist-get field :error-message))
          (should-not (plist-get field :error-overlay))))))))

(ert-deftest clutch-test-insert-idle-validation-covers-all-changed-fields ()
  "Idle validation should validate every field changed since the last run."
  (clutch-test--with-insert-result-buffer result-buf
      (:columns '("impact_score" "severity")
       :column-defs '((:name "impact_score" :type-category numeric)
                      (:name "severity" :type-category numeric))
       :connection 'fake-conn)
    (clutch-test--with-pop-to-buffer-capture insert-buf
      (cl-letf (((symbol-function 'clutch--ensure-column-details)
                 (lambda (_conn _table)
                   (list (list :name "impact_score" :type "decimal(5,1)")
                         (list :name "severity" :type "int")))))
        (clutch-result-insert--open-buffer "shipping_incidents" result-buf)
        (with-current-buffer insert-buf
          (clutch-test--goto-insert-field-value "impact_score")
          (insert "x")
          (clutch-test--goto-insert-field-value "severity")
          (insert "1")
          (let ((timer (buffer-local-value 'clutch-result-insert--validation-timer
                                           insert-buf)))
            (apply (timer--function timer) (timer--args timer)))
          (should (equal (plist-get (clutch-result-insert--field-state "impact_score")
                                    :error-message)
                         "Field impact_score expects a numeric value"))
          (should-not (plist-get (clutch-result-insert--field-state "severity")
                                 :error-message))
          (clutch-test--goto-insert-field-value "impact_score")
          (delete-region (point) (line-end-position))
          (insert "1.5")
          (clutch-test--goto-insert-field-value "severity" t)
          (insert "2")
          (let ((timer (buffer-local-value 'clutch-result-insert--validation-timer
                                           insert-buf)))
            (apply (timer--function timer) (timer--args timer)))
          (let ((field (clutch-result-insert--field-state "impact_score")))
            (should-not (plist-get field :error-message))
            (should-not (plist-get field :error-overlay))))))))

(ert-deftest clutch-test-json-validation-is-scheduled-on-idle ()
  "JSON insert and edit buffers should defer local validation until idle."
  (dolist (case '(insert edit))
    (ert-info ((format "case: %s" case))
      (let (scheduled)
        (cl-letf (((symbol-function 'run-with-idle-timer)
                   (lambda (secs _repeat fn &rest args)
                     (setq scheduled (list secs fn args))
                     'fake-timer)))
          (pcase case
            ('insert
             (clutch-test--with-insert-result-buffer result-buf
                 (:columns '("postmortem")
                  :column-defs '((:name "postmortem" :type-category json))
                  :connection 'fake-conn)
               (clutch-test--with-pop-to-buffer-capture insert-buf
                 (cl-letf (((symbol-function 'clutch--ensure-column-details)
                            (lambda (_conn _table)
                              (list (list :name "postmortem" :type "json")))))
                   (clutch-result-insert--open-buffer
                    "shipping_incidents" result-buf)
                   (with-current-buffer insert-buf
                     (clutch-test--goto-insert-field-value "postmortem")
                     (insert "{")
                     (should scheduled)
                     (should (= (car scheduled)
                                clutch-insert-validation-idle-delay))
                     (should (eq (cadr scheduled)
                                 #'clutch-result-insert--run-idle-validation))
                     (should (equal (caddr scheduled)
                                    (list (current-buffer)))))))))
            ('edit
             (with-temp-buffer
               (clutch--result-edit-mode 1)
               (setq-local clutch-result-edit--column-name "payload"
                           clutch-result-edit--column-def
                           '(:name "payload" :type-category json)
                           clutch-result-edit--column-detail
                           '(:name "payload" :type "json"))
               (clutch-result-edit--schedule-validation)
               (should scheduled)
               (should (= (car scheduled) clutch-insert-validation-idle-delay))
               (should (eq (cadr scheduled)
                           #'clutch-result-edit--run-idle-validation))
               (should (equal (caddr scheduled)
                              (list (current-buffer))))))))))))

(ert-deftest clutch-test-edit-live-validation-updates-header ()
  "Edit buffers should update the compact live-validation token."
  (with-temp-buffer
    (insert "xx")
    (clutch--result-edit-mode 1)
    (setq-local clutch-result-edit--column-name "impact_score"
                clutch-result-edit--column-def '(:name "impact_score" :type-category numeric)
                clutch-result-edit--column-detail '(:name "impact_score" :type "decimal(5,1)"))
    (clutch-result-edit--refresh-header-line)
    (clutch-result-edit--validate-live)
    (should (equal clutch-result-edit--error-message
                   "Field impact_score expects a numeric value"))
    (should (string-match-p "\\[invalid numeric\\]"
                            (format "%s" header-line-format)))
    (erase-buffer)
    (insert "1.5")
    (clutch-result-edit--validate-live)
    (should-not clutch-result-edit--error-message)
    (should-not (string-match-p "\\[invalid numeric\\]"
                                (format "%s" header-line-format)))))

(ert-deftest clutch-test-edit-finish-validates-before-stage ()
  "Edit staging should reject invalid values before calling the edit callback."
  (dolist (case '((numeric "xx" "impact_score"
                   (:name "impact_score" :type-category numeric)
                   (:name "impact_score" :type "decimal(5,1)")
                   "Field impact_score expects a numeric value")
                  (enum "urgent" "severity"
                   (:name "severity" :type-category text)
                   (:name "severity" :type "enum('low','medium','high')")
                   "Field severity must be one of: low, medium, high")
                  (json "{oops}" "payload"
                   (:name "payload" :type-category json)
                   (:name "payload" :type "json")
                   "Field payload expects valid JSON")))
    (pcase-let ((`(,label ,value ,column-name ,column-def
                   ,column-detail ,message) case))
      (ert-info ((format "case: %s" label))
        (let (staged-value quit-called err)
          (clutch-test--with-result-edit-buffer edit-buf value
            (setq-local clutch-result-edit--column-name column-name
                        clutch-result-edit--column-def column-def
                        clutch-result-edit--column-detail column-detail
                        clutch-result--edit-callback
                        (lambda (staged) (setq staged-value staged)))
            (cl-letf (((symbol-function 'quit-window)
                       (lambda (&rest _args) (setq quit-called t))))
              (setq err (should-error (clutch-result-edit-finish)
                                      :type 'user-error))
              (should (string-match-p (regexp-quote message)
                                      (error-message-string err)))
              (should-not quit-called)
              (should-not staged-value)
              (should (buffer-live-p edit-buf)))))))))

(ert-deftest clutch-test-edit-set-null-stages-nil ()
  "The explicit NULL command should stage a nil value."
  (let ((staged-value :not-called)
        quit-called)
    (clutch-test--with-result-edit-buffer _edit-buf "12.5"
      (setq-local clutch-result-edit--column-name "impact_score"
                  clutch-result-edit--column-def '(:name "impact_score" :type-category numeric)
                  clutch-result-edit--column-detail
                  '(:name "impact_score" :type "decimal(5,1)" :nullable t)
                  clutch-result--edit-callback
                  (lambda (value) (setq staged-value (list value))))
      (cl-letf (((symbol-function 'quit-window)
                 (lambda (&rest _args) (setq quit-called t))))
        (clutch-result-edit-set-null)
        (clutch-result-edit-finish)
        (should quit-called)
        (should (equal staged-value '(nil)))))))

(ert-deftest clutch-test-edit-set-default-stages-explicit-sentinel ()
  "The explicit DEFAULT command should stage the database-default sentinel."
  (let ((staged-value :not-called)
        quit-called)
    (clutch-test--with-result-edit-buffer _edit-buf "manual"
      (setq-local clutch-result-edit--column-name "status"
                  clutch-result-edit--column-def
                  '(:name "status" :type-category text)
                  clutch-result-edit--column-detail
                  '(:name "status" :type "text" :nullable t :default "'new'")
                  clutch-result-edit--default-supported-p t
                  clutch-result--edit-callback
                  (lambda (value) (setq staged-value value)))
      (cl-letf (((symbol-function 'quit-window)
                 (lambda (&rest _args) (setq quit-called t))))
        (should (eq (lookup-key clutch--result-edit-mode-map (kbd "C-c C-d"))
                    #'clutch-result-edit-set-default))
        (clutch-result-edit-set-default)
        (should (eq clutch-result-edit--special-value 'default))
        (should (equal (buffer-string) ""))
        (should (equal
                 (substring-no-properties
                  (overlay-get clutch-result-edit--special-placeholder-overlay
                               'after-string))
                 "<default>"))
        (should (eq (get-text-property
                     0 'face
                     (overlay-get clutch-result-edit--special-placeholder-overlay
                                  'after-string))
                    'clutch-null-face))
        (clutch-result-edit-finish)
        (should quit-called)
        (should (eq staged-value clutch--cell-default-placeholder))))))

(ert-deftest clutch-test-edit-finish-preserves-literal-special-strings ()
  "Typing NULL or DEFAULT should stage literal text, not a special value."
  (dolist (text '("NULL" "DEFAULT"))
    (let (staged-value)
      (clutch-test--with-result-edit-buffer _edit-buf text
        (setq-local clutch-result-edit--column-name "note"
                    clutch-result-edit--column-def
                    '(:name "note" :type-category text)
                    clutch-result-edit--column-detail
                    '(:name "note" :type "text" :nullable t :default "'x'")
                    clutch-result-edit--default-supported-p t
                    clutch-result--edit-callback
                    (lambda (value) (setq staged-value value)))
        (cl-letf (((symbol-function 'quit-window) #'ignore))
          (clutch-result-edit-finish)
          (should (equal staged-value text)))))))

(ert-deftest clutch-test-edit-finish-restores-result-cell-position ()
  "Finishing a cell edit should restore point without shifting the viewport."
  (save-window-excursion
    (let ((result-buf (generate-new-buffer "*clutch-result-test*"))
          edit-buf)
      (unwind-protect
          (progn
            (switch-to-buffer result-buf)
            (clutch-test--init-result-state
             (list :columns '("id" "name" "city" "note" "flag")
                   :rows '((1 "alpha" "oslo" "before" "x")
                           (2 "bravo" "rome" "target" "y"))
                   :page-total-rows 2
                   :column-widths [3 18 18 18 18]
                   :render t
                   :row-identity
                   (clutch-test--primary-row-identity "users" '("id") '(0))))
            (setq-local clutch-connection nil
                        clutch--connection-params '(:backend mysql)
                        clutch--result-source-table "users")
            (clutch--goto-cell 1 4)
            (set-window-hscroll (selected-window) 40)
            (cl-letf (((symbol-function 'clutch--ensure-column-details)
                       (lambda (&rest _)
                         '((:name "id") (:name "name") (:name "city")
                           (:name "note") (:name "flag"))))
                      ((symbol-function 'window-body-width)
                       (lambda (&rest _) 40)))
              (clutch-result-edit-cell))
            (setq edit-buf (current-buffer))
            (erase-buffer)
            (insert "z")
            (cl-letf (((symbol-function 'quit-window)
                       (lambda (&rest _args)
                         (switch-to-buffer result-buf)
                         (set-window-hscroll (selected-window) 0)))
                      ((symbol-function 'window-body-width)
                       (lambda (&rest _) 40)))
              (clutch-result-edit-finish))
            (should (eq (current-buffer) result-buf))
            (should (= (get-text-property (point) 'clutch-row-idx) 1))
            (should (= (get-text-property (point) 'clutch-col-idx) 4))
            (should (= (window-hscroll) 40)))
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
                      clutch--result-column-defs
                      '((:name "name" :type-category text
                         :source-column "name"))
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
                     (lambda (&rest _) '((:name "name"))))
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

(ert-deftest clutch-test-insert-stage-validates-fields-before-stage ()
  "Insert staging should reject invalid enum, bool, JSON, temporal, and numeric values."
  (dolist (case
           `((("severity" "is_ship_blocked" "postmortem")
              ((:name "severity" :type-category text)
               (:name "is_ship_blocked" :type-category numeric)
               (:name "postmortem" :type-category json))
              (("severity" . "nope")
               ("is_ship_blocked" . "7")
               ("postmortem" . "not-json"))
              ((:name "severity" :type "enum('low','medium')")
               (:name "is_ship_blocked" :type "tinyint(1)")
               (:name "postmortem" :type "json"))
              nil)
             (("opened_at" "due_on" "starts_at")
              ((:name "opened_at" :type-category datetime)
               (:name "due_on" :type-category date)
               (:name "starts_at" :type-category time))
              (("opened_at" . "ss")
               ("due_on" . "2026-02-30")
               ("starts_at" . "25:61"))
              ((:name "opened_at" :type "datetime")
               (:name "due_on" :type "date")
               (:name "starts_at" :type "time"))
              "Field opened_at expects YYYY-MM-DD HH:MM\\[:SS\\]")
             (("impact_score")
              ((:name "impact_score" :type-category numeric))
              (("impact_score" . "xx"))
              ((:name "impact_score" :type "decimal(5,1)"))
              "Field impact_score expects a numeric value")))
    (pcase-let ((`(,columns ,column-defs ,fields ,details ,expected-message) case))
      (clutch-test--with-insert-result-buffer result-buf
          (:columns columns
           :column-defs column-defs
           :connection 'fake-conn
           :source-table "shipping_incidents"
           :pending-inserts nil)
        (clutch-test--with-pop-to-buffer-capture insert-buf
          (cl-letf (((symbol-function 'clutch--ensure-column-details)
                     (lambda (_conn _table) details)))
            (clutch-result-insert--open-buffer
             "shipping_incidents" result-buf fields)
            (with-current-buffer insert-buf
              (let ((err (should-error (clutch-result-insert-stage)
                                       :type 'user-error)))
                (when expected-message
                  (should (string-match-p expected-message
                                          (error-message-string err))))))))
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
                 (lambda (_language &optional _quiet) nil))
                ((symbol-function 'js-mode)
                 (lambda () (setq selected-mode 'js-mode))))
        (clutch-result-insert--json-editor-mode)))
    (should (eq selected-mode 'js-mode))))

(ert-deftest clutch-test-json-sub-editor-preserves-serialization-errors ()
  "JSON save commands distinguish invalid input from serialization failure."
  (dolist (command '(clutch-result-edit-json-finish
                     clutch-result-insert-json-finish))
    (ert-info ((symbol-name command))
      (with-temp-buffer
        (setq-local clutch-result-edit-json--field-name "payload"
                    clutch-result-insert-json--field-name "payload")
        (insert "{")
        (should (equal
                 (should-error (call-interactively command) :type 'user-error)
                 '(user-error "Field payload expects valid JSON")))
        (erase-buffer)
        (insert "{\"ok\":true}")
        (cl-letf (((symbol-function 'json-serialize)
                   (lambda (&rest _) (error "Serializer failed"))))
          (should (equal
                   (should-error (call-interactively command)
                                 :type 'clutch-db-error)
                   '(clutch-db-error
                     "Cannot serialize value as JSON: Serializer failed"))))
        (should (equal (buffer-string) "{\"ok\":true}"))))))

(ert-deftest clutch-test-json-sub-editor-editing-contract ()
  "JSON child editors should validate parents, save JSON, and cancel cleanly."
  (dolist (case
           '((save
              ""
              "{\n  \"severity\": \"high\",\n  \"ship_blocked\": true\n}"
              clutch-result-insert-json-finish
              "{\"severity\":\"high\",\"ship_blocked\":true}")
             (cancel
              "{\"ok\":true}"
              "{\"ok\":false}"
              clutch-result-insert-json-cancel
              "{\"ok\":true}")))
    (pcase-let ((`(,label ,parent-value ,editor-text ,command ,expected) case))
      (ert-info ((format "case: %s" label))
        (clutch-test--with-pop-to-buffer-capture editor-buf
          (clutch-test--with-insert-result-buffer result-buf
              (:columns '("postmortem")
               :column-defs '((:name "postmortem" :type-category json))
               :connection 'fake-conn)
            (let (insert-buf)
              (cl-letf (((symbol-function 'clutch--ensure-column-details)
                         (lambda (_conn _table)
                           (list (list :name "postmortem" :type "json")))))
                (clutch-result-insert--open-buffer
                 "shipping_incidents" result-buf
                 `(("postmortem" . ,parent-value)))
                (setq insert-buf editor-buf)
                (with-current-buffer insert-buf
                  (clutch-test--goto-insert-field-value "postmortem")
                  (clutch-result-insert-edit-json-field)))
              (with-current-buffer editor-buf
                (erase-buffer)
                (insert editor-text)
                (cl-letf (((symbol-function 'clutch--ensure-column-details)
                           (lambda (_conn _table)
                             (list (list :name "postmortem" :type "json"))))
                          ((symbol-function 'quit-window)
                           (lambda (&rest _args) nil))
                          ((symbol-function 'pop-to-buffer)
                           (lambda (buf &rest _args) buf)))
                  (funcall command)))
              (with-current-buffer insert-buf
                (should (equal
                         (plist-get
                          (clutch-result-insert--field-state "postmortem")
                          :value)
                         expected)))
              (when (buffer-live-p insert-buf)
                (kill-buffer insert-buf))))))))
  (ert-info ("insert editor rejects invalid parent field text")
    (clutch-test--with-pop-to-buffer-capture editor-buf
      (clutch-test--with-insert-result-buffer result-buf
          (:columns '("postmortem")
           :column-defs '((:name "postmortem" :type-category json))
           :connection 'fake-conn)
        (let (insert-buf)
          (cl-letf (((symbol-function 'clutch--ensure-column-details)
                     (lambda (_conn _table)
                       (list (list :name "postmortem" :type "json")))))
            (clutch-result-insert--open-buffer
             "shipping_incidents" result-buf '(("postmortem" . "hello")))
            (setq insert-buf editor-buf)
            (setq editor-buf nil)
            (with-current-buffer insert-buf
              (clutch-test--goto-insert-field-value "postmortem")
              (let ((err (should-error (clutch-result-insert-edit-json-field)
                                       :type 'user-error)))
                (should (string-match-p "Field postmortem expects valid JSON"
                                        (error-message-string err))))))
          (should-not editor-buf)
          (with-current-buffer insert-buf
            (should (equal
                     (plist-get (clutch-result-insert--field-state "postmortem")
                                :value)
                     "hello")))
          (when (buffer-live-p insert-buf)
            (kill-buffer insert-buf))))))
  (ert-info ("edit editor saves normalized JSON")
    (clutch-test--with-pop-to-buffer-capture json-buf
      (clutch-test--with-result-edit-buffer parent-buf "{\"a\":1}"
        (setq-local clutch-result-edit--column-name "payload"
                    clutch-result-edit--column-def
                    '(:name "payload" :type-category json)
                    clutch-result-edit--column-detail
                    '(:name "payload" :type "json"))
        (clutch-result-edit-json-field)
        (with-current-buffer json-buf
          (erase-buffer)
          (insert "{\"a\":2}")
          (clutch-result-edit-json-finish))
        (should (equal (with-current-buffer parent-buf (buffer-string))
                       "{\"a\":2}")))))
  (ert-info ("manual edit editor cancel returns to parent edit buffer")
    (let (json-buf popped-buf)
      (clutch-test--with-result-edit-buffer parent-buf "{\"a\":1}"
        (setq-local clutch-result-edit--column-name "payload"
                    clutch-result-edit--column-def
                    '(:name "payload" :type-category json)
                    clutch-result-edit--column-detail
                    '(:name "payload" :type "json"))
        (cl-letf (((symbol-function 'pop-to-buffer)
                   (lambda (buf &rest _args)
                     (setq json-buf buf)
                     buf)))
          (clutch-result-edit-json-field))
        (should (buffer-live-p json-buf))
        (with-current-buffer json-buf
          (should-not clutch-result-edit-json--whole-edit-p))
        (cl-letf (((symbol-function 'quit-window)
                   (lambda (&optional kill _window)
                     (when kill
                       (kill-buffer (current-buffer)))))
                  ((symbol-function 'pop-to-buffer)
                   (lambda (buf &rest _args)
                     (setq popped-buf buf)
                     buf)))
          (with-current-buffer json-buf
            (clutch-result-edit-json-cancel)))
        (should-not (buffer-live-p json-buf))
        (should (buffer-live-p parent-buf))
        (should (eq popped-buf parent-buf))
        (should (equal (with-current-buffer parent-buf (buffer-string))
                       "{\"a\":1}")))))
  (ert-info ("edit editor rejects invalid parent text")
    (clutch-test--with-result-edit-buffer _parent-buf "hello"
      (setq-local clutch-result-edit--column-name "payload"
                  clutch-result-edit--column-def
                  '(:name "payload" :type-category json)
                  clutch-result-edit--column-detail
                  '(:name "payload" :type "json"))
      (cl-letf (((symbol-function 'pop-to-buffer)
                 (lambda (&rest _args)
                   (ert-fail "JSON editor should not open"))))
        (let ((err (should-error (clutch-result-edit-json-field)
                                 :type 'user-error)))
          (should (string-match-p "Field payload expects valid JSON"
                                  (error-message-string err))))))))

;;;; Export — dispatch and content

(ert-deftest clutch-test-export-command-writes-selected-format-content ()
  "Export command should write the chosen format through the real export path."
  (dolist (case '(("c" nil clipboard "id,name\n1,\"a,b\"\n" nil)
                  ("c" ("--no-header") clipboard "1,\"a,b\"\n" nil)
                  ("c" ("--file") file "id,name\n1,\"a,b\"\n" "CSV")
                  ("t" nil clipboard "id\tname\n1\ta,b\n" nil)
                  ("t" ("--no-header") clipboard "1\ta,b\n" nil)
                  ("t" ("--file") file "id\tname\n1\ta,b\n" "TSV")
                  ("i" nil clipboard
                   "INSERT INTO \"users\" (\"id\", \"name\") VALUES (1, 'a,b');\n"
                   nil)
                  ("i" ("--file") file
                   "INSERT INTO \"users\" (\"id\", \"name\") VALUES (1, 'a,b');\n"
                   nil)
                  ("u" nil clipboard
                   "UPDATE \"users\" SET \"name\" = 'a,b' WHERE \"id\" = 1\n"
                   nil)
                  ("u" ("--file") file
                   "UPDATE \"users\" SET \"name\" = 'a,b' WHERE \"id\" = 1\n"
                   nil)))
    (pcase-let ((`(,key ,args ,target ,expected ,expected-coding-label) case))
      (ert-info ((format "export key: %s, args: %S" key args))
        (let ((path (make-temp-file "clutch-export-"))
              (kill-ring nil)
              (kill-ring-yank-pointer nil)
              (write-region-function (symbol-function 'write-region))
              coding-label
              written-coding)
          (unwind-protect
              (clutch-test--with-result-state
                  (:columns '("id" "name")
                   :column-defs '((:name "id" :type-category numeric)
                                  (:name "name" :type-category text))
                   :connection-params '(:backend mysql)
                   :source-table "users"
                   :last-query "SELECT id, name FROM users"
                   :row-identity
                   (clutch-test--primary-row-identity
                    "users" '("id") '(0)))
                (let ((suffix
                       (clutch-test--transient-suffix-for-key
                        'clutch-result-export key)))
                  (should suffix)
                  (cl-letf (((symbol-function 'transient-args)
                             (lambda (_prefix) args))
                            ((symbol-function 'completing-read)
                             (lambda (prompt choices &rest _args)
                               (if (string-match
                                    "\\`\\(CSV\\|TSV\\) encoding" prompt)
                                   (progn
                                     (should (member "utf-8" choices))
                                     (setq coding-label
                                           (match-string 1 prompt))
                                     "utf-8")
                                 (ert-fail
                                  (format "Unexpected prompt: %s" prompt)))))
                            ((symbol-function 'read-file-name)
                             (lambda (&rest _args) path))
                            ((symbol-function 'write-region)
                             (lambda (&rest write-args)
                               (setq written-coding coding-system-for-write)
                               (apply write-region-function write-args)))
                            ((symbol-function 'clutch-result--collect-all-export-rows)
                             (lambda (on-rows) (funcall on-rows '((1 "a,b")))))
                            ((symbol-function 'clutch-result--map-export-batches)
                             (lambda (function done)
                               (funcall function '((1 "a,b")))
                               (funcall done nil)))
                            ((symbol-function 'clutch--ensure-column-details)
                             (lambda (_conn _table &optional _strict)
                               (list (list :name "id")
                                     (list :name "name"))))
                            ((symbol-function 'clutch-db-escape-identifier)
                             (lambda (_conn s) (format "\"%s\"" s)))
                            ((symbol-function 'clutch-db-escape-literal)
                             (lambda (_conn s) (format "'%s'" s))))
                    (funcall (oref suffix command))
                    (should (equal coding-label expected-coding-label))
                    (when expected-coding-label
                      (should (eq written-coding 'utf-8)))
                    (should
                     (equal (if (eq target 'clipboard)
                                (current-kill 0)
                              (with-temp-buffer
                                (insert-file-contents path)
                                (buffer-string)))
                            expected)))))
            (ignore-errors (delete-file path))))))))

(ert-deftest clutch-test-export-transient-shares-copy-option-model ()
  "Export should expose Header and Destination before choosing a format."
  (let* ((header
          (clutch-test--transient-suffix-for-key 'clutch-result-export "-h"))
         (destination
          (clutch-test--transient-suffix-for-key 'clutch-result-export "-f"))
         (header-display (and header (transient-format-value header)))
         (destination-display
          (and destination (transient-format-value destination))))
    (should header)
    (should destination)
    (should (equal (substring-no-properties header-display) "(No|Yes)"))
    (should (eq (get-text-property (string-match "Yes" header-display)
                                  'face header-display)
                'transient-value))
    (should (equal (substring-no-properties destination-display)
                   "(Clipboard|File)"))
    (should (eq (get-text-property
                 (string-match "Clipboard" destination-display)
                 'face destination-display)
                'transient-value))))

(ert-deftest clutch-test-csv-content-escaping ()
  "CSV content should include header and escaped values."
  (with-temp-buffer
    (setq-local clutch--result-columns '("id" "display,name"))
    (let ((csv (clutch--export-csv-content
                '((1 "a,b") (2 "x\"y") (3 "x\ry")))))
      (should (string-match-p "^id,\"display,name\"\n" csv))
      (should (string-match-p "1,\"a,b\"" csv))
      (should (string-match-p "2,\"x\"\"y\"" csv))
      (should (string-match-p "3,\"x\ry\"" csv)))
    (should (equal (clutch--export-csv-content '((1 "a,b")) t)
                   "1,\"a,b\"\n"))))

(ert-deftest clutch-test-tsv-content-includes-header-and-escapes-fields ()
  "TSV content should preserve its tabular shape around special characters."
  (with-temp-buffer
    (setq-local clutch--result-columns '("id" "display\tname"))
    (should
     (equal (clutch--export-tsv-content
             '((1 "a,b") (2 "x\ty") (3 "x\"y") (4 "x\ny")))
            (concat
             "id\t\"display\tname\"\n"
             "1\ta,b\n"
             "2\t\"x\ty\"\n"
             "3\t\"x\"\"y\"\n"
             "4\t\"x\ny\"\n")))
    (should (equal (clutch--export-tsv-content '((1 "a,b")) t)
                   "1\ta,b\n"))))

(ert-deftest clutch-test-insert-content-builds-full-row-sql ()
  :tags '(:smoke)
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
                clutch--result-column-defs
                '((:name "id" :type-category numeric :source-column "id")
                  (:name "name" :type-category text :source-column "name"))
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

(ert-deftest clutch-test-result-export-formats-follow-result-surface ()
  "Export choices should match SQL, document, and key/value result surfaces."
  (cl-labels
      ((render-menu ()
         (when-let* ((buffer (get-buffer " *transient*")))
           (kill-buffer buffer))
         (transient-setup 'clutch-result-export)
         (with-current-buffer " *transient*"
           (should (string-match-p "Export all result rows" (buffer-string)))
           (buffer-string)))
       (check-menu (menu present absent)
         (dolist (label present)
           (should (string-match-p (regexp-quote label) menu)))
         (dolist (label absent)
           (should-not (string-match-p (regexp-quote label) menu)))))
    (unwind-protect
        (progn
          (clutch-test--with-native-document-result-buffer
            (cl-letf (((symbol-function
                        'clutch-db-document-mutation-supported-p)
                       (lambda (_conn action) (eq action 'insert-many))))
              (check-menu (render-menu)
                          '("CSV" "TSV" "Insert many")
                          '("INSERT SQL" "UPDATE SQL"))))
          (with-temp-buffer
            (setq-local clutch-connection 'sql-conn
                        clutch--connection-params nil)
            (clutch-test--with-connection-data-model
                ('sql-conn 'mysql 'relational)
              (check-menu (render-menu)
                          '("CSV" "TSV" "INSERT SQL" "UPDATE SQL")
                          '("Insert many"))))
          (with-temp-buffer
            (setq-local clutch-connection 'redis-conn
                        clutch--connection-params nil)
            (clutch-test--with-connection-data-model
                ('redis-conn 'redis 'key-value)
              (check-menu (render-menu)
                          '("CSV" "TSV")
                          '("INSERT SQL" "UPDATE SQL"
                            "Insert many")))))
      (when-let* ((buffer (get-buffer " *transient*")))
        (kill-buffer buffer)))))

(ert-deftest clutch-test-copy-export-menus-show-data-scope ()
  "Menus distinguish current cell, selection and locally filtered export."
  (clutch-test--with-result-state
   (:connection nil :connection-params '(:backend sqlite) :render t)
   (let ((transient-mark-mode t))
     (unwind-protect
         (progn
           (goto-char (point-min))
           (transient-setup 'clutch-result-copy-dispatch)
           (with-current-buffer " *transient*"
             (should (string-match-p "Copy current cell" (buffer-string))))
           (push-mark (point-max) nil t)
           (transient-setup 'clutch-result-copy-dispatch)
           (with-current-buffer " *transient*"
             (should (string-match-p "Copy selected cells" (buffer-string))))
           (deactivate-mark)
           (setq-local clutch--filter-pattern "alice")
           (transient-setup 'clutch-result-export)
           (with-current-buffer " *transient*"
             (should (string-match-p "ignores local filter" (buffer-string)))))
       (when-let* ((buffer (get-buffer " *transient*")))
         (kill-buffer buffer))))))

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

(ert-deftest clutch-test-non-sql-results-reject-sql-mutation-commands ()
  "Document and key/value results should reject SQL-only mutation commands."
  (dolist (surface '(document key-value))
    (dolist (case '((copy "Copy INSERT SQL is SQL-only")
                    (edit "Edit / re-edit is SQL-only")))
      (pcase-let ((`(,op ,expected-message) case))
        (pcase surface
          ('document
           (clutch-test--with-native-document-result-buffer
             (let ((err (pcase op
                          ('copy (should-error (clutch-result-copy 'insert)
                                               :type 'user-error))
                          ('edit (should-error (clutch-result-edit-cell)
                                               :type 'user-error)))))
               (should (string-match-p expected-message
                                       (error-message-string err))))))
          ('key-value
           (with-temp-buffer
             (setq-local clutch-connection 'redis-conn
                         clutch--connection-params nil)
             (clutch-test--with-connection-data-model
                 ('redis-conn 'redis 'key-value)
               (let ((err (pcase op
                            ('copy (should-error (clutch-result-copy 'insert)
                                                 :type 'user-error))
                            ('edit (should-error (clutch-result-edit-cell)
                                                 :type 'user-error)))))
                 (should (string-match-p expected-message
                                         (error-message-string err))))))))))))

(ert-deftest clutch-test-insert-sql-uses-placeholder-for-ambiguous-source ()
  "INSERT copy/export should use a placeholder table for ambiguous queries."
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
      (should (equal (current-kill 0)
                     "INSERT INTO MY_TABLE (id) VALUES (1);"))
      (setq-local clutch--result-columns '("id" "name"))
      (should (equal (clutch--export-insert-content '((1 "a") (2 "b")))
                     (concat
                      "INSERT INTO MY_TABLE (id, name) VALUES (1, 'a');\n"
                      "INSERT INTO MY_TABLE (id, name) VALUES (2, 'b');\n"))))))

(ert-deftest clutch-test-pg-array-mutation-builders-use-array-literals ()
  "PostgreSQL array mutations should render JSON-style edits as array literals."
  (require 'clutch-db-pg)
  (require 'pgsql)
  (let ((conn (clutch-db-pg--make-connection :client 'fake-pgsql-client)))
    (clutch-test--with-result-state
        (:connection conn
         :source-table "models"
         :columns '("id" "precision")
         :column-defs '((:name "id" :type-category numeric)
                        (:name "precision" :type-category text
                         :backend-type "_int4"))
         :rows '((1 [0 1]))
         :row-identity (clutch-test--primary-row-identity
                        "models" '("id") '(0))
         :pending-edits
         (list (cons (cons (vector 1) 1) "[0,1,2]")))
      (cl-letf (((symbol-function 'clutch--ensure-column-details)
                 (lambda (_conn _table &optional _strict)
                   '((:name "id" :type "integer" :backend-type "int4")
                     (:name "precision" :type "ARRAY"
                      :backend-type "_int4")))))
        (should (equal (clutch-result--pending-sql-statements)
                       '("UPDATE \"models\" SET \"precision\" = '{0,1,2}' WHERE \"id\" = 1")))
        (should (equal
                 (clutch-result--render-statements
                  (list (clutch-result-insert--build-sql
                         conn "models" '(("precision" . "[0,1,2]")))))
                 '("INSERT INTO \"models\" (\"precision\") VALUES ('{0,1,2}')")))
        (should (equal
                 (clutch-result--build-insert-statements-for-rows
                  '((1 [0 1 2])) '(1) "models")
                 '("INSERT INTO \"models\" (\"precision\") VALUES ('{0,1,2}');")))))))

(ert-deftest clutch-test-copy-update-uses-selection ()
  "UPDATE copy should generate SQL from the active row/column selection."
  (dolist (case
           `(("region rectangle"
              t ((0 1) . (1 2)) nil
              ,(concat
                "UPDATE \"users\" SET \"name\" = 'a', \"status\" = 'new' WHERE \"id\" = 1\n"
                "UPDATE \"users\" SET \"name\" = 'b', \"status\" = 'done' WHERE \"id\" = 2"))
             ("current cell"
              nil nil (0 1 "a")
              "UPDATE \"users\" SET \"name\" = 'a' WHERE \"id\" = 1")))
    (pcase-let ((`(,label ,region-p ,rectangle ,cell ,expected) case))
      (ert-info ((format "copy UPDATE selection: %s" label))
        (with-temp-buffer
          (let (kill-ring kill-ring-yank-pointer)
            (setq-local clutch-connection 'fake-conn
                        clutch--result-source-table "users"
                        clutch--result-columns '("id" "name" "status")
                        clutch--result-column-defs
                        '((:name "id" :type-category numeric
                           :source-column "id")
                          (:name "name" :type-category text
                           :source-column "name")
                          (:name "status" :type-category text
                           :source-column "status"))
                        clutch--row-identity (clutch-test--primary-row-identity
                                              "users" '("id") '(0))
                        clutch--result-rows '((1 "a" "new")
                                              (2 "b" "done")))
            (cl-letf (((symbol-function 'use-region-p) (lambda () region-p))
                      ((symbol-function 'clutch-result--region-rectangle-indices)
                       (lambda () rectangle))
                      ((symbol-function 'clutch--cell-at-point)
                       (lambda () cell))
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
              (should (equal (current-kill 0) expected)))))))))

(ert-deftest clutch-test-copy-builders-use-filtered-visible-row ()
  "Copy builders should resolve visible indices through filtered display rows."
  (clutch-test--with-result-state
      (:columns '("id" "name")
       :rows '((1 "alpha") (2 "beta"))
       :filter-pattern "beta"
       :filtered-rows '((2 "beta")))
    (cl-letf (((symbol-function 'clutch-db-escape-identifier)
               (lambda (_conn s) s))
              ((symbol-function 'clutch-db-escape-literal)
               (lambda (_conn s) (format "'%s'" s)))
              ((symbol-function 'use-region-p) (lambda () nil))
              ((symbol-function 'clutch--cell-at-point)
               (lambda () (list 0 1 "beta")))
              ((symbol-function 'clutch-result--build-update-statements-for-rows)
               (lambda (rows col-indices op)
                 (should (equal rows '((2 "beta"))))
                 (should (equal col-indices '(1)))
                 (should (equal op "copy UPDATE SQL"))
                 '("UPDATE users SET name = 'beta' WHERE id = 2"))))
      (clutch-result--copy-rows 'update)
      (should (equal (clutch--delimited-lines-for-rows
                      (clutch-result--rows-for-display-indices '(0)) '(1) ?,)
                     '("name" "beta")))
      (should (equal (clutch-result--build-insert-statements-for-rows
                      (clutch-result--rows-for-display-indices '(0))
                      '(1) "users")
                     '("INSERT INTO users (name) VALUES ('beta');"))))))

(ert-deftest clutch-test-copy-update-rejects-non-writable-selections ()
  "UPDATE copy should reject selections that cannot produce writable SET columns."
  (dolist (case
           (list
            (list :label "pk only"
                  :columns '("id" "name")
                  :defs '((:name "id" :type-category numeric)
                          (:name "name" :type-category text))
                  :row '(1 "a")
                  :indices '(0)
                  :details '((:name "id") (:name "name"))
                  :message "Cannot copy UPDATE SQL: no writable source columns selected")
            (list :label "computed result column"
                  :columns '("id" "name" "computed_total")
                  :defs '((:name "id" :type-category numeric)
                          (:name "name" :type-category text)
                          (:name "computed_total" :type-category numeric))
                  :row '(1 "alice" 42)
                  :indices '(0 1 2)
                  :details '((:name "id") (:name "name"))
                  :message "Cannot copy UPDATE SQL: selected columns are not writable source columns: computed_total")
            (list :label "generated source column"
                  :columns '("id" "generated_name")
                  :defs '((:name "id" :type-category numeric)
                          (:name "generated_name" :type-category text))
                  :row '(1 "alice")
                  :indices '(0 1)
                  :details '((:name "id") (:name "generated_name" :generated t))
                  :message "Cannot copy UPDATE SQL: selected columns are not writable source columns: generated_name")))
    (ert-info ((format "copy UPDATE rejection: %s" (plist-get case :label)))
      (with-temp-buffer
        (setq-local clutch-connection 'fake-conn
                    clutch--result-columns (plist-get case :columns)
                    clutch--result-column-defs
                    (cl-mapcar
                     (lambda (name definition)
                       (plist-put (copy-sequence definition)
                                  :source-column name))
                     (plist-get case :columns)
                     (plist-get case :defs))
                    clutch--result-source-table "users"
                    clutch--row-identity (clutch-test--primary-row-identity
                                          "users" '("id") '(0)))
        (cl-letf (((symbol-function 'clutch--ensure-column-details)
                   (lambda (_conn _table &optional _strict)
                     (plist-get case :details))))
          (let ((err (should-error
                      (clutch-result--build-update-statements-for-rows
                       (list (plist-get case :row))
                       (plist-get case :indices)
                       "copy UPDATE SQL")
                      :type 'user-error)))
            (should (string-match-p
                     (regexp-quote (plist-get case :message))
                     (error-message-string err)))))))))

(ert-deftest clutch-test-staged-update-rejects-non-writable-source-columns ()
  "A staged edit should not build an UPDATE of a column no longer writable.
Editing refuses such a column, so this happens when the table changes
after the edit is staged."
  (dolist (case '(("generated" ((:name "id") (:name "name" :generated t)))
                  ("missing" ((:name "id")))))
    (ert-info ((car case))
      (with-temp-buffer
        (setq-local clutch-connection 'fake-conn
                    clutch--result-columns '("id" "name")
                    clutch--result-column-defs
                    '((:name "id" :backend-type "int4" :source-column "id")
                      (:name "name" :source-column "name"))
                    clutch--result-source-table "users"
                    clutch--row-identity (clutch-test--primary-row-identity
                                          "users" '("id") '(0))
                    clutch--pending-edits '((([1] . 1) . "alice")))
        (cl-letf (((symbol-function 'clutch--ensure-column-details)
                   (lambda (_conn _table &optional _strict) (cadr case)))
                  ((symbol-function 'clutch-db-escape-identifier)
                   (lambda (_conn name) name)))
          (should (equal (error-message-string
                          (should-error (clutch-result--build-update-statements)
                                        :type 'user-error))
                         "Cannot build UPDATE: selected columns are not writable source columns: name")))))))

(ert-deftest clutch-test-copy-pending-sql-copies-current-batch ()
  "Staged SQL copy should mirror the staged submit batch."
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
  "Staged SQL save should write the staged submit batch to disk."
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

(defun clutch-test--copy-agent-context ()
  "Run `clutch-copy-context-for-agent' with deterministic metadata."
  (let ((clutch--table-metadata-cache (make-hash-table :test 'eq))
        (clutch--object-cache (make-hash-table :test 'eq))
        copied)
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
               (lambda (_conn table &optional _schema)
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
                               :target-table "users" :unique t)))))
              ((symbol-function 'kill-new)
               (lambda (text) (setq copied text))))
      (clutch-copy-context-for-agent))
    copied))

(ert-deftest clutch-test-copy-context-for-agent-from-query-console ()
  "Agent context copy should use the public query-console command path."
  (dolist (case '(("normal" "SELECT id, email FROM users WHERE active = 1" t)
                  ("trailing semicolon"
                   "SELECT id, email FROM users WHERE active = 1;" nil)))
    (pcase-let ((`(,label ,sql ,check-metadata) case))
      (ert-info ((format "case: %s" label))
        (with-temp-buffer
          (clutch-mode)
          (insert sql)
          (setq-local clutch-connection 'fake-conn)
          (let ((copied (clutch-test--copy-agent-context)))
            (should (string-match-p
                     "SELECT id, email FROM users WHERE active = 1" copied))
            (should (string-match-p "## Table: users" copied))
            (when check-metadata
              (should (string-match-p "# Clutch database context" copied))
              (should (string-match-p "- Backend: PostgreSQL" copied))
              (should (string-match-p "users (TABLE)" copied))
              (should (string-match-p "Comment\n  application users" copied))
              (should (string-match-p "Columns (2)" copied))
              (should (string-match-p
                       "id[[:space:]]+bigint[[:space:]]+NOT NULL, PK"
                       copied))
              (should (string-match-p
                       "email[[:space:]]+text[[:space:]]+NOT NULL, FK -> orgs.id, login email"
                       copied))
              (should (string-match-p "Indexes (1)" copied))
              (should (string-match-p "users_email_idx[[:space:]]+UNIQUE"
                                      copied))
              (should-not (string-match-p "Row identity candidates"
                                          copied)))))))))

(ert-deftest clutch-test-result-mode-k-copies-agent-context ()
  "The documented result-mode k binding should copy agent context."
  (should (eq (lookup-key clutch-result-mode-map "k")
              #'clutch-copy-context-for-agent)))

(ert-deftest clutch-test-result-mode-scales-header-with-buffer-text ()
  "Result headers should follow buffer-local text scaling exactly once."
  (require 'face-remap)
  (let ((text-scale-remap-header-line nil))
    (with-temp-buffer
      (clutch-result-mode)
      (should (local-variable-p 'text-scale-remap-header-line))
      (should text-scale-remap-header-line)
      (dolist (face '(mode-line mode-line-inactive))
        (let ((spec (cadr (assq face face-remapping-alist))))
          (should-not (eq (plist-get spec :inherit) 'default)))))))

(ert-deftest clutch-test-result-mode-draws-header-in-the-row-font ()
  "Result headers should keep the rows' font under a proportional theme.
A theme such as modus with `modus-themes-variable-pitch-ui' gives the
header line a proportional font, while header padding follows the rows.
Only the family is remapped, so text scaling still applies once."
  (with-temp-buffer
    (clutch-result-mode)
    (let ((spec (cadr (assq 'header-line face-remapping-alist))))
      (should (equal (plist-get spec :family)
                     (face-attribute 'default :family nil t)))
      (should-not (plist-member spec :inherit)))))

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
        (cl-letf (((symbol-function 'mouse-drag-region)
                   (lambda (_event) (setq delegated 'drag)))
                  ((symbol-function 'mouse-set-point)
                   (lambda (_event) (setq delegated 'set-point))))
          (clutch-result-mouse-set-point
           (list 'mouse-1
                 (list (selected-window) event-position '(0 . 0) 0)))
          (should (= (point) 2))
          (should-not delegated)
          (setq event-position 1)
          (clutch-result-mouse-set-point
           (list 'down-mouse-1
                 (list (selected-window) event-position '(0 . 0) 0)))
          (should (eq delegated 'drag))
          (clutch-result-mouse-set-point
           (list 'mouse-1
                 (list (selected-window) event-position '(0 . 0) 0)))
          (should (eq delegated 'set-point)))))))

(ert-deftest clutch-test-copy-context-for-agent-from-result-buffer-includes-sample ()
  "Agent context copy should include effective result SQL and current sample rows."
  (let ((clutch-agent-context-max-result-rows 1)
        copied)
    (clutch-test--with-result-state
        (:base-query "SELECT id, email FROM users"
         :where-filter "active = 1"
         :source-table "users"
         :columns '("clutch__rid_0" "id" "email")
         :column-defs '((:name "clutch__rid_0" :hidden t)
                        (:name "id")
                        (:name "email"))
         :rows (list (vector "rid-1" 1 "ada@example.com")
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
            (clutch-test--init-result-state
             (list :base-query "SELECT id, email FROM users"
                   :columns '("id" "email")
                   :rows (list (vector 1 "ada@example.com")
                               (vector 2 "bob@example.com")))))
          (setq copied (clutch-test--copy-agent-context))
          ;; The sample-section details are proven by the result-buffer
          ;; test above; here the rule is that a console resolves its
          ;; last result buffer at all.
          (should (string-match-p "## Result sample" copied))
          (should (string-match-p "1\tada@example.com" copied)))
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

(ert-deftest clutch-test-aggregate-selection-scenarios ()
  "Aggregate should summarize current, filtered, and rectangular selections."
  (dolist (case '((current nil
                   ("id" "score")
                   ((1 "1.5") (2 "2.5") (3 "x") (4 4))
                   nil nil nil (1 1 "2.5")
                   ("Aggregate \\[score\\]" "sum=2.5" "avg=2.5"
                    "\\[rows=1 cells=1 skipped=0\\]")
                   nil)
                  (filtered nil
                   ("id" "score")
                   ((1 10) (2 20))
                   "20" ((2 20)) nil (0 1 20)
                   ("sum=20")
                   ("sum=10"))
                  (region-multi t
                   ("id" "a" "b")
                   ((1 10 20) (2 11 21))
                   nil nil ((0 1) 1 2) nil
                   ("Aggregate \\[selection\\]" "sum=62" "avg=15.5"
                    "\\[rows=2 cells=4 skipped=0\\]")
                   nil)
                  (region-single t
                   ("id" "score")
                   ((1 "1") (2 "2") (3 "3"))
                   nil nil ((0 2) 1) (0 1 "1")
                   ("Aggregate \\[score\\]" "sum=4" "avg=2"
                    "\\[rows=2 cells=2 skipped=0\\]")
                   nil)
                  (scientific t
                   ("id" "score")
                   ((1 "1E+3") (2 "2.5"))
                   nil nil ((0 1) 1) (0 1 "1E+3")
                   ("sum=1002.5" "\\[rows=2 cells=2 skipped=0\\]")
                   nil)))
    (pcase-let ((`(,name ,region-active ,columns ,rows ,filter ,filtered
                         ,rect ,cell ,expected ,absent)
                 case))
      (ert-info ((symbol-name name))
        (clutch-test--with-result-state
            (:columns columns
             :rows rows
             :filter-pattern filter
             :filtered-rows filtered)
          (let (kill-ring kill-ring-yank-pointer)
            (cl-letf (((symbol-function 'use-region-p)
                       (lambda () region-active))
                      ((symbol-function 'clutch-result--region-rectangle-indices)
                       (lambda () rect))
                      ((symbol-function 'clutch--cell-at-point)
                       (lambda () cell)))
              (clutch-result-aggregate)
              (let ((summary (current-kill 0)))
                (dolist (pattern expected)
                  (should (string-match-p pattern summary)))
                (dolist (pattern absent)
                  (should-not (string-match-p pattern summary)))))))))))

(ert-deftest clutch-test-aggregate-refreshes-footer-without-redrawing-body ()
  "Aggregate should update footer state without rebuilding the result body."
  (clutch-test--with-result-state
      (:columns '("id" "score")
       :rows '((1 10) (2 20)))
    (let ((footer-refreshes 0)
          (body-refreshes 0)
          kill-ring
          kill-ring-yank-pointer)
      (cl-letf (((symbol-function 'use-region-p) (lambda () nil))
                ((symbol-function 'clutch--cell-at-point)
                 (lambda () '(1 1 20)))
                ((symbol-function 'clutch--refresh-footer-line)
                 (lambda () (cl-incf footer-refreshes)))
                ((symbol-function 'clutch--refresh-display)
                 (lambda () (cl-incf body-refreshes))))
        (clutch-result-aggregate)
        (should (= footer-refreshes 1))
        (should (= body-refreshes 0))
        (should (= (plist-get clutch--aggregate-summary :sum) 20))))))

(ert-deftest clutch-test-aggregate-with-prefix-refines-region ()
  "Prefix-arg aggregate should use refined rectangle selection."
  (clutch-test--with-result-state
      (:columns '("id" "score")
       :rows '((1 "1") (2 "2") (3 "3")))
    (let (kill-ring kill-ring-yank-pointer)
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

(ert-deftest clutch-test-tsv-copy-selection-contract ()
  "TSV copy should use the selected columns with optional headers."
  (dolist (case `((t ((0 1) . (0 2)) nil nil
                     "id\tstate\n1\t<default>\n2\tactive")
                  (nil nil (0 1 "alice") nil "name\nalice")
                  (nil nil (0 2 ,clutch--cell-default-placeholder) t
                       "<default>")))
    (pcase-let ((`(,region-active ,rect ,cell ,omit-header ,expected) case))
      (ert-info ((format "region: %s" region-active))
        (clutch-test--with-result-state
            (:columns '("id" "name" "state")
             :rows `((1 "alice" ,clutch--cell-default-placeholder)
                     (2 "bob" "active")))
          (let (kill-ring kill-ring-yank-pointer)
            (cl-letf (((symbol-function 'use-region-p)
                       (lambda () region-active))
                      ((symbol-function 'clutch-result--region-rectangle-indices)
                       (lambda () rect))
                      ((symbol-function 'clutch--cell-at-point)
                       (lambda () cell)))
              (clutch-result-copy 'tsv nil omit-header)
              (should (equal (current-kill 0) expected)))))))))

(ert-deftest clutch-test-copy-format-commands-copy-visible-content ()
  "Public CSV and TSV copy commands should copy through the real entry point."
  (dolist (case '((clutch-result-copy-csv "name\nalice")
                  (clutch-result-copy-org-table "| name  |\n|-------|\n| alice |")
                  (clutch-result-copy-tsv "name\nalice")))
    (pcase-let ((`(,command ,expected-text) case))
      (ert-info ((symbol-name command))
        (clutch-test--with-result-state
            (:columns '("id" "name")
             :rows '((1 "alice")))
          (let (kill-ring kill-ring-yank-pointer)
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

(ert-deftest clutch-test-copy-header-switch-omits-tabular-headers ()
  "Copy commands should honor the installed no-header switch."
  (dolist (case '((clutch-result-copy-tsv "alice")
                  (clutch-result-copy-csv "alice")
                  (clutch-result-copy-org-table "| alice |")))
    (pcase-let ((`(,command ,expected) case))
      (clutch-test--with-result-state
          (:columns '("name")
           :rows '(("alice")))
        (let (kill-ring kill-ring-yank-pointer)
          (cl-letf (((symbol-function 'transient-args)
                     (lambda (_prefix) '("--no-header")))
                    ((symbol-function 'transient-arg-value)
                     (lambda (flag args) (member flag args)))
                    ((symbol-function 'use-region-p) (lambda () nil))
                    ((symbol-function 'clutch--cell-at-point)
                     (lambda () '(0 0 "alice"))))
            (funcall command)
            (should (equal (current-kill 0) expected))))))))

(ert-deftest clutch-test-copy-header-switch-defaults-to-yes ()
  "Copy transient should present Header as enabled by default."
  (let* ((suffix
          (clutch-test--transient-suffix-for-key
           'clutch-result-copy-dispatch "-h"))
         (display (and suffix (transient-format-value suffix))))
    (should suffix)
    (should (equal (substring-no-properties display) "(No|Yes)"))
    (should (eq (get-text-property (string-match "Yes" display)
                                  'face display)
                'transient-value))))

(ert-deftest clutch-test-copy-fmt-with-refine-uses-refined-rectangle ()
  "Refined copy should copy the final rectangle, not the initial region."
  (clutch-test--with-result-state
      (:columns '("id" "name" "score")
       :rows '((1 "alice" 10)
               (2 "bob" 20)
               (3 "cam" 30)))
    (let (kill-ring kill-ring-yank-pointer)
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
  (clutch-test--with-result-state
      (:columns '("city" "amount" "note")
       :column-defs '((:name "city" :type-category text)
                      (:name "amount" :type-category numeric)
                      (:name "note" :type-category text))
       :rows '(("sh" 1 "a|b")
               ("Tokyo" 200 "x\ny")))
    (let (kill-ring kill-ring-yank-pointer)
      (cl-letf (((symbol-function 'use-region-p) (lambda () t))
                ((symbol-function 'clutch-result--region-rectangle-indices)
                 (lambda () '((0 1) . (0 1 2)))))
        (clutch-result-copy 'org-table)
        (should (equal (current-kill 0)
                       (concat "| city  | amount | note    |\n"
                               "|-------+--------+---------|\n"
                               "| sh    |      1 | a\\vertb |\n"
                               "| Tokyo |    200 | x\\ny    |")))))))

(ert-deftest clutch-test-copy-delimited-unified-entry-uses-selection ()
  "CSV/TSV copy uses the selection without validating each value twice."
  (dolist (case '((t ((0 1) 1 2) nil "c1,c2\na1,a2\nb1,b2" 6)
                  (nil nil (0 1 a1) "c1\na1" 2)))
    (pcase-let ((`(,region-active ,rect ,cell ,expected ,max-checks) case))
      (clutch-test--with-result-state
          (:columns '("c0" "c1" "c2")
                    :rows '((a0 a1 a2)
                            (b0 b1 b2)
                            (c0 c1 c2)))
        (dolist (kind '(csv tsv))
          (let ((checks 0)
                (check (symbol-function 'clutch-db-require-complete-value))
                kill-ring kill-ring-yank-pointer)
            (cl-letf (((symbol-function 'clutch-db-require-complete-value)
                       (lambda (value) (cl-incf checks) (funcall check value)))
                      ((symbol-function 'use-region-p)
                       (lambda () region-active))
                      ((symbol-function 'clutch-result--region-rectangle-indices)
                       (lambda () rect))
                      ((symbol-function 'clutch--cell-at-point)
                       (lambda () cell)))
              (clutch-result-copy kind)
              (should (equal (current-kill 0)
                             (if (eq kind 'tsv)
                                 (subst-char-in-string ?, ?\t expected)
                               expected)))
              (should (<= checks max-checks)))))))))

(ert-deftest clutch-test-copy-insert-unified-entry-uses-selection ()
  "Unified INSERT copy should use either the active region or current cell."
  (dolist (case
           '((t ((0 1) 0 1) nil
              ("INSERT INTO \"t\" (\"id\", \"name\") VALUES ('1', 'a');"
               "INSERT INTO \"t\" (\"id\", \"name\") VALUES ('2', 'b');"))
	     (nil nil (0 1 "a")
	      ("INSERT INTO \"t\" (\"name\") VALUES ('a');"))))
    (pcase-let ((`(,region-active ,rect ,cell ,expected-lines) case))
      (clutch-test--with-result-state
          (:connection-params '(:backend mysql)
           :columns '("id" "name" "age")
           :rows '((1 "a" 10) (2 "b" 20))
           :last-query "SELECT id, name, age FROM t")
        (let (kill-ring kill-ring-yank-pointer)
          (cl-letf (((symbol-function 'use-region-p)
                     (lambda () region-active))
                    ((symbol-function 'clutch-result--region-rectangle-indices)
                     (lambda () rect))
                    ((symbol-function 'clutch--cell-at-point)
                     (lambda () cell))
                    ((symbol-function 'clutch-db-escape-identifier)
                     (lambda (_conn s) (format "\"%s\"" s)))
                    ((symbol-function 'clutch-db-value-to-literal)
                     (lambda (_conn v &optional _formatter)
                       (format "'%s'" v))))
            (clutch-result-copy 'insert)
            (if region-active
                (dolist (expected expected-lines)
                  (should (string-match-p (regexp-quote expected)
                                          (current-kill 0))))
              (should (equal (current-kill 0) (car expected-lines))))))))))

;;;; Refine

(defun clutch-test--setup-refine-result-buffer ()
  "Populate the current buffer with a small rendered result table."
  (clutch-test--init-result-state
   '(:columns ("id" "name")
     :column-defs ((:name "id" :type-category numeric)
                   (:name "name" :type-category text))
     :rows ((1 "alice-long-value") (2 "bob") (3 "carol"))
     :column-widths [2 5]))
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

(ert-deftest clutch-test-refine-suspends-automatic-cell-preview ()
  "Refine mode should close and suppress automatic cell previews."
  (with-temp-buffer
    (clutch-test--setup-refine-result-buffer)
    (goto-char (point-min))
    (let ((match (text-property-search-forward
                  'clutch-cell-truncated t #'eq)))
      (should match)
      (goto-char (prop-match-beginning match)))
    (let ((clutch-cell-preview-style 'child-frame)
          (clutch--cell-preview-state
           (list :source-buffer (current-buffer)))
          (clutch--cell-preview-timer 'pending)
          cancelled
          scheduled)
      (cl-letf (((symbol-function 'cancel-timer)
                 (lambda (timer) (setq cancelled timer)))
                ((symbol-function 'clutch--cell-preview-supported-p)
                 (lambda (_window) t))
                ((symbol-function 'run-with-idle-timer)
                 (lambda (&rest _args)
                   (setq scheduled t)
                   nil)))
        (clutch-result--start-refine '((0) . (0 1)) #'ignore)
        (should (eq cancelled 'pending))
        (should-not clutch--cell-preview-timer)
        (should-not clutch--cell-preview-state)
        (clutch--schedule-cell-preview)
        (should-not scheduled)
        (clutch-refine-cancel)
        (clutch--schedule-cell-preview)
        (should scheduled)))))

(ert-deftest clutch-test-refine-toggle-excludes-and-includes ()
  "Refine mode should toggle row and column exclusions at point."
  (dolist (case '((row clutch-row-idx clutch-refine-toggle-row
                       clutch--refine-excluded-rows)
                  (column clutch-col-idx clutch-refine-toggle-col
                          clutch--refine-excluded-cols)))
    (pcase-let ((`(,label ,property ,toggle ,state-var) case))
      (ert-info ((format "case: %s" label))
        (with-temp-buffer
          (clutch-test--setup-refine-result-buffer)
          (clutch-result--start-refine '((0 1 2) . (0 1)) #'ignore)
          (goto-char (point-min))
          (let ((match (text-property-search-forward property 1 #'eq)))
            (should match)
            (goto-char (prop-match-beginning match)))
          (funcall toggle)
          (should (equal (symbol-value state-var) '(1)))
          (funcall toggle)
          (should-not (symbol-value state-var)))))))

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

(ert-deftest clutch-test-refine-confirm-errors-when-selection-empty ()
  "Refine confirm should error when row or column exclusions empty the selection."
  (dolist (case '((all-rows ((0) . (0 1)) clutch-row-idx 0
                            clutch-refine-toggle-row)
                  (all-cols ((0 1) . (0)) clutch-col-idx 0
                            clutch-refine-toggle-col)))
    (pcase-let ((`(,label ,rect ,property ,value ,toggle) case))
      (ert-info ((format "case: %s" label))
        (with-temp-buffer
          (clutch-test--setup-refine-result-buffer)
          (clutch-result--start-refine rect #'ignore)
          (goto-char (point-min))
          (let ((match (text-property-search-forward property value #'eq)))
            (should match)
            (goto-char (prop-match-beginning match)))
          (funcall toggle)
          (should-error (clutch-refine-confirm) :type 'user-error))))))

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

(ert-deftest clutch-test-page-navigation-contract ()
  "Page commands should error at boundaries and dispatch target pages."
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
          (should-error (funcall command) :type 'user-error)))))
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

(ert-deftest clutch-test-last-page-navigation ()
  "Last page should calculate final windows and reject boundary states."
  (dolist (case '((normal 0 nil 237 50 4 187 nil)
                  (already-last 4 187 237 50 nil nil user-error)
                  (shift-to-last-window 1 500 578 500 1 78 nil)
                  (single-page 0 nil 30 50 nil nil user-error)))
    (pcase-let ((`(,label ,current ,offset ,total ,page-size
                          ,expected-page ,expected-offset ,error-type)
                 case))
      (ert-info ((symbol-name label))
        (with-temp-buffer
          (setq-local clutch--page-current current
                      clutch--page-offset offset
                      clutch--page-total-rows total
                      clutch-result-max-rows page-size)
          (let (executed-page executed-offset)
            (cl-letf (((symbol-function 'clutch-result--execute-page)
                       (lambda (page &optional offset)
                         (setq executed-page page
                               executed-offset offset))))
              (if error-type
                  (should-error (clutch-result-last-page) :type error-type)
                (clutch-result-last-page)
                (should (= executed-page expected-page))
                (should (= executed-offset expected-offset))))))))))

(defmacro clutch-test--with-instant-pages (pages-var &rest body)
  "Run BODY with every result page load succeeding at once.
Each loaded page number is pushed onto PAGES-VAR, and every page holds the
result's current rows."
  (declare (indent 1) (debug (symbolp body)))
  `(let ((,pages-var nil))
     (cl-letf (((symbol-function 'clutch--ensure-connection) #'ignore)
               ((symbol-function 'clutch-db-build-paged-sql)
                (lambda (_conn _sql page-num &rest _)
                  (push page-num ,pages-var)
                  "SELECT 1"))
               ((symbol-function 'clutch-result--run-query)
                (lambda (_sql _query on-result)
                  (funcall on-result
                           (make-clutch-db-result
                            :columns clutch--result-column-defs
                            :rows clutch--result-rows)
                           0)))
               ((symbol-function 'clutch--refresh-display) #'ignore)
               ((symbol-function 'message) #'ignore))
       ,@body)))

(ert-deftest clutch-test-sort-by-column-state-machine ()
  "Keyboard sorting should cycle column state and reject non-column points."
  (dolist (case '((toggle "name" 1 "name" nil nil nil ("name" t nil))
                  (clear "name" 1 "name" t ("name" . "DESC") clear nil)
                  (new-column "age" 2 "name" t nil nil ("age" nil nil))
                  (no-column nil nil nil nil nil error nil)))
    (pcase-let ((`(,label ,text ,col-idx ,sort-column ,sort-descending
                         ,order-by ,expected-state ,expected-sort)
                 case))
      (ert-info ((symbol-name label))
        (clutch-test--with-result-state
            (:columns '("id" "name" "age")
             :column-defs '((:name "id")
                            (:name "name")
                            (:name "age"))
             :server-rewritable t
             :server-pageable t
             :base-query "SELECT id, name, age FROM t"
             :sort-column sort-column
             :sort-descending sort-descending
             :page-current 4)
          (when text
            (insert text)
            (add-text-properties (point-min) (point-max)
                                 (list 'clutch-col-idx col-idx))
            (goto-char (point-min)))
          (setq-local clutch--order-by order-by)
          (let (sort-args)
            (clutch-test--with-instant-pages pages
              (cl-letf (((symbol-function 'completing-read)
                         (lambda (&rest _)
                           (error "unexpected sort column prompt")))
                        ((symbol-function 'clutch-result--sort)
                         (lambda (col desc &optional idx)
                           (setq sort-args (list col desc idx)))))
                (pcase expected-state
                  ('error
                   (let ((err (should-error (clutch-result-sort-by-column)
                                            :type 'user-error)))
                     (should (string-match-p "No column at point"
                                             (error-message-string err)))))
                  ('clear
                   (clutch-result-sort-by-column)
                   (should-not clutch--sort-column)
                   (should-not clutch--sort-descending)
                   (should-not clutch--order-by)
                   (should (= clutch--page-current 0))
                   (should (equal pages '(0)))
                   (should-not sort-args))
                  (_
                   (clutch-result-sort-by-column)
                   (should (equal sort-args expected-sort))))))))))))

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

(ert-deftest clutch-test-query-console-sqli-keys ()
  "Console keys use Clutch results instead of an unrelated SQLi process."
  (let ((conn (clutch-db-connect 'sqlite '(:database ":memory:")))
        (other (clutch-db-connect 'sqlite '(:database ":memory:")))
        (source (generate-new-buffer " *clutch-console-keys*"))
        result)
    (unwind-protect
        (save-window-excursion
          (switch-to-buffer source)
          (clutch-mode)
          (setq-local clutch-connection conn)
          (should (eq (key-binding (kbd "C-c C-n")) #'undefined))
          (should-error (call-interactively (key-binding (kbd "C-c C-z")))
                        :type 'user-error)
          (dolist (sql '("CREATE TABLE probe (id INTEGER)" "SELECT 1"))
            (switch-to-buffer source)
            (erase-buffer)
            (insert sql)
            (clutch-execute-buffer)
            (setq result (get-buffer (clutch-result--buffer-name)))
            (with-current-buffer result
              (should (derived-mode-p 'clutch-result-mode)))
            (switch-to-buffer source)
            (call-interactively (key-binding (kbd "C-c C-z")))
            (should (eq (window-buffer (selected-window)) result)))
          ;; A closed result window comes back below the console, where
          ;; queries show results, even where the frame would split sideways.
          (switch-to-buffer source)
          (delete-other-windows)
          (let ((split-width-threshold 20))
            (call-interactively (key-binding (kbd "C-c C-z"))))
          (let ((below (window-in-direction 'below (get-buffer-window source))))
            (should below)
            (should (eq (window-buffer below) result)))
          ;; The result belongs to this connection, not every SQL console.
          (switch-to-buffer source)
          (let ((clutch-connection other))
            (should-error (call-interactively (key-binding (kbd "C-c C-z")))
                          :type 'user-error))
          (kill-buffer result)
          (should-error (call-interactively (key-binding (kbd "C-c C-z")))
                        :type 'user-error))
      (when (buffer-live-p result) (kill-buffer result))
      (kill-buffer source)
      (clutch-db-disconnect other)
      (when (clutch-db-live-p conn) (clutch-db-disconnect conn)))))

(ert-deftest clutch-test-query-dispatches-route-x-to-dwim ()
  "SQL and MongoDB dispatch menus should share the DWIM execute route."
  (should (eq (lookup-key clutch-mode-map (kbd "C-c ?"))
              #'clutch-dispatch))
  (should (eq (lookup-key clutch-mongodb-mode-map (kbd "C-c ?"))
              #'clutch-mongodb-dispatch))
  (dolist (prefix '(clutch-dispatch clutch-mongodb-dispatch))
    (ert-info ((format "prefix: %s" prefix))
      (let ((execute
             (cl-find-if
              (lambda (suffix)
                (and (slot-boundp suffix 'key)
                     (equal (oref suffix key) "x")))
              (transient-suffixes prefix))))
        (should execute)
        (should (eq (oref execute command) #'clutch-execute-dwim))))))

(ert-deftest clutch-test-edit-transient-heading-shows-staged-count ()
  "Edit transient heading should summarize staged mutation count."
  (with-temp-buffer
    (setq-local clutch--pending-edits '(edit-a edit-b)
                clutch--pending-deletes '(delete-a)
                clutch--pending-inserts '(insert-a insert-b insert-c))
    (should (equal (substring-no-properties
                    (clutch-result--edit-transient-heading))
                   "Edit (6 staged)"))
    (setq-local clutch--pending-edits nil
                clutch--pending-deletes nil
                clutch--pending-inserts nil)
    (should (equal (clutch-result--edit-transient-heading) "Edit"))))

(ert-deftest clutch-test-result-dispatch-pending-actions-follow-state ()
  "Result dispatch should show pending actions only while changes are staged."
  (with-temp-buffer
    (cl-letf (((symbol-function 'clutch-result--action-supported-p)
               (lambda (action) (eq action 'sql-mutation))))
        (cl-labels ((render-menu ()
                      (when-let* ((buffer (get-buffer " *transient*")))
                        (kill-buffer buffer))
                      (transient-setup 'clutch-result-dispatch)
                      (with-current-buffer " *transient*"
                        (buffer-string))))
          (unwind-protect
              (let ((labels '("Submit staged"
                              "Discard staged at point"
                              "Copy staged SQL"
                              "Save staged SQL"))
                    (menu (render-menu)))
                (dolist (label labels)
                  (should-not (string-match-p (regexp-quote label) menu)))
                (setq-local clutch--pending-edits '(pending))
                (setq menu (render-menu))
                (dolist (label labels)
                  (should (string-match-p (regexp-quote label) menu))))
            (when-let* ((buffer (get-buffer " *transient*")))
              (kill-buffer buffer)))))))

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

(ert-deftest clutch-test-sort-transient-description-contract ()
  "Result sort transient description should reflect current sort context."
  (clutch-test--with-result-state
      (:columns '("created_at")
       :column-defs '((:name "created_at"))
       :server-rewritable t)
    (insert "created_at")
    (add-text-properties (point-min) (point-max) '(clutch-col-idx 0))
    (goto-char (point-min))
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
                   "Sort current (no column)")))
  (clutch-test--with-result-state
      (:columns '("id" "name" "age")
       :column-defs '((:name "id")
                      (:name "name")
                      (:name "age"))
       :server-rewritable t
       :sort-column "name"
       :sort-descending t)
    (insert "age")
    (add-text-properties (point-min) (point-max) '(clutch-col-idx 2))
    (goto-char (point-min))
    (let ((desc (substring-no-properties
                 (clutch-result--sort-transient-description))))
      (should (string-match-p "Sort current" desc))
      (should (string-match-p "\\[age\\]" desc))
      (should (string-match-p "(none|asc|desc)" desc))))
  (clutch-test--with-result-state
      (:columns '("score" "score")
       :column-defs '((:name "score") (:name "score"))
       :server-rewritable nil
       :sort-column "score"
       :sort-descending t)
    (insert "score")
    (add-text-properties (point-min) (point-max) '(clutch-col-idx 0))
    (goto-char (point-min))
    (setq-local clutch--local-sort-column-index 1)
    (let ((desc (clutch-result--sort-transient-description)))
      (should (string-match-p "Sort page" desc))
      (should (eq (get-text-property (string-match "none" desc) 'face desc)
                  'transient-value)))
    (put-text-property (point-min) (point-max) 'clutch-col-idx 1)
    (let ((desc (clutch-result--sort-transient-description)))
      (should (eq (get-text-property (string-match "desc" desc) 'face desc)
                  'transient-value)))))

(ert-deftest clutch-test-sort-by-header-column-contract ()
  "Header sorting should use captured names and cycle sort state."
  (clutch-test--with-result-state
      (:columns '("id" "name")
       :server-rewritable t
       :server-pageable t
       :base-query "SELECT id, name FROM t")
    (clutch-test--with-instant-pages pages
      (clutch-result--sort-by-column-index 99 "name")
      (should (equal clutch--sort-column "name"))
      (should (equal clutch--order-by '("name" . "ASC")))
      (should (equal pages '(0)))))
  (clutch-test--with-result-state
      (:columns '("id" "age")
       :column-defs '((:name "id") (:name "age")))
    (let (sort-args)
      (cl-letf (((symbol-function 'clutch-result--sort)
                 (lambda (name descending)
                   (setq sort-args (list name descending)))))
        (let ((err (should-error
                    (clutch-result--sort-by-column-index 1 "name")
                    :type 'user-error)))
          (should (string-match-p "Column not found"
                                  (error-message-string err)))))
      (should-not sort-args)))
  (clutch-test--with-result-state
      (:columns '("id" "name")
       :server-rewritable t
       :server-pageable t
       :base-query "SELECT id, name FROM t"
       :page-current 3)
    (clutch-test--with-instant-pages pages
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
      (should (equal (nreverse pages) '(0 0 0 0))))))

(ert-deftest clutch-test-local-sort-cycles-on-an-empty-page ()
  "Sorting an empty page locally should cycle back to unsorted."
  (clutch-test--with-result-state
      (:columns '("id" "name")
       :rows nil)
    (cl-letf (((symbol-function 'clutch--refresh-display) #'ignore)
              ((symbol-function 'message) #'ignore))
      (clutch-result--sort-by-column-index 1)
      (should (equal clutch--sort-column "name"))
      (clutch-result--sort-by-column-index 1)
      (should clutch--sort-descending)
      (clutch-result--sort-by-column-index 1)
      (should-not clutch--sort-column)
      (should-not clutch--sort-descending))))

(ert-deftest clutch-test-sort-rejects-hidden-row-identity-column ()
  "Server-side sort should only accept visible user columns."
  (clutch-test--with-result-state
      (:columns '("clutch__rid_0" "id" "name")
       :column-defs '((:name "clutch__rid_0" :hidden t)
                      (:name "id")
                      (:name "name"))
       :server-rewritable t)
    (let (paged)
      (cl-letf (((symbol-function 'clutch-result--execute-page)
                 (lambda (&rest _args) (setq paged t))))
        (let ((err (should-error (clutch-result--sort "clutch__rid_0" nil)
                                 :type 'user-error)))
          (should (string-match-p "Column clutch__rid_0 not found"
                                  (error-message-string err))))
        (should-not paged)))))

(ert-deftest clutch-test-sort-falls-back-to-current-page-for-nonrewritable-result ()
  "Arbitrary query results should cycle a local current-page sort."
  (clutch-test--with-result-state
      (:columns '("score" "score")
       :column-defs '((:name "score") (:name "score"))
       :rows '((1 20) (2 nil) (3 10) (4 10))
       :server-rewritable nil)
    (insert "score")
    (add-text-properties (point-min) (point-max) '(clutch-col-idx 1))
    (goto-char (point-min))
    (let ((refreshed 0))
      (cl-letf (((symbol-function 'clutch-result--execute-page)
                 (lambda (&rest _args)
                   (ert-fail "local sort must not execute a query")))
                ((symbol-function 'clutch--refresh-display)
                 (lambda () (cl-incf refreshed)))
                ((symbol-function 'message) #'ignore))
        (clutch-result--sort-by-column-index 1)
        (should (equal (mapcar #'car clutch--result-rows) '(2 3 4 1)))
        (should (equal clutch--local-sort-original-rows
                       '((1 20) (2 nil) (3 10) (4 10))))
        (should (equal clutch--sort-column "score"))
        (should-not clutch--sort-descending)
        (should-not clutch--order-by)
        (should (= clutch--local-sort-column-index 1))
        (should (string-match-p
                 "Sort page"
                 (clutch-result--sort-transient-description)))
        (should (string-match-p
                 "ASC\\[score\\] page"
                 (substring-no-properties (clutch--footer-sort-part))))
        (clutch-result--sort-by-column-index 1)
        (should (equal (mapcar #'car clutch--result-rows) '(1 3 4 2)))
        (should clutch--sort-descending)
        (should (= clutch--local-sort-column-index 1))
        (clutch-result--sort-by-column-index 1)
        (should (equal (mapcar #'car clutch--result-rows) '(1 2 3 4)))
        (should-not clutch--local-sort-original-rows)
        (should-not clutch--local-sort-column-index)
        (should-not clutch--sort-column)
        (should (= refreshed 3))))))

(ert-deftest clutch-test-local-numeric-sort-keeps-exact-decimal-order ()
  "Local numeric sorting should not use text order or floating point."
  (with-temp-buffer
    (setq-local clutch--result-columns '("amount")
                clutch--result-column-defs
                '((:name "amount" :type-category numeric))
                clutch--local-sort-original-rows
                '(("12345678901234567890.12345678901234567891")
                  ("10")
                  (nil)
                  ("2")
                  ("12345678901234567890.12345678901234567890"))
                clutch--filter-pattern nil)
    (clutch-result--sort-local-page "amount" nil)
    (should
     (equal clutch--result-rows
            '((nil)
              ("2")
              ("10")
              ("12345678901234567890.12345678901234567890")
              ("12345678901234567890.12345678901234567891"))))))

(ert-deftest clutch-test-local-numeric-sort-orders-floats-and-sentinels ()
  "Local numeric sorting should order exponent floats, infinity, and NaN."
  (with-temp-buffer
    (setq-local clutch--result-columns '("label" "amount")
                clutch--result-column-defs
                '((:name "label" :type-category text)
                  (:name "amount" :type-category numeric))
                clutch--local-sort-original-rows
                (list (list 'two 2)
                      (list 'nan 0.0e+NaN)
                      (list 'negative -1e+20)
                      (list 'infinity 1.0e+INF))
                clutch--filter-pattern nil)
    (clutch-result--sort-local-page "amount" nil 1)
    (should (equal (mapcar #'car clutch--result-rows)
                   '(negative two infinity nan)))))

;;;; Execute — query execution and error handling

(ert-deftest clutch-test-execute-only-paginates-select-statements ()
  "Result-set commands must not inherit SELECT pagination rewrites."
  (dolist (case
           '(("SELECT * FROM users" t)
             ("-- rows\nWITH active AS (SELECT * FROM users) SELECT * FROM active" t)
             ("SHOW INDEX FROM users WHERE Key_name = 'users_name_key'" nil)
             ("DESCRIBE users" nil)
             ("DESC users" nil)
             ("EXPLAIN SELECT * FROM users" nil)
             ("PRAGMA table_info(users)" nil)
             ("VALUES (1)" nil)
             ("INSERT INTO users (name) VALUES ('Ada') RETURNING id" nil)
             ("WITH i AS (INSERT INTO users (name) VALUES ('Ada') RETURNING id) SELECT * FROM i" nil)
             ("SELECT * INTO users_copy FROM users" nil)
             ("SELECT * FROM FINAL TABLE (INSERT INTO users (name) VALUES ('Ada'))" nil)
             ("CALL list_users()" nil)))
    (pcase-let ((`(,sql ,pageable) case))
      (ert-info ((format "sql: %s" sql))
        (let (executed-sql identity-prepared)
          (cl-letf (((symbol-function 'clutch--prepare-row-identity-query)
                     (lambda (_connection statement)
                       (setq identity-prepared t)
                       (list :sql statement)))
                    ((symbol-function 'clutch-db-build-paged-sql)
                     (lambda (_connection statement _page-num _page-size
                              &optional _order-by _page-offset)
                       (concat statement " /* paged */")))
                    ((symbol-function 'clutch--run-db-query)
                     (lambda (_connection statement)
                       (setq executed-sql statement)
                       (make-clutch-db-result
                        :columns '((:name "value"))
                        :rows '((1))))))
            (let ((outcome
                   (clutch-test--await-outcome
                    (lambda (k)
                      (clutch--execute-statement-attempt
                       sql 'fake-conn t nil nil k)))))
              (should (plist-get outcome :result-query-p))
              (should (eq (plist-get outcome :server-pageable) pageable))
              (should (eq (and identity-prepared t) pageable))
              (should (equal executed-sql
                             (if pageable
                                 (concat sql " /* paged */")
                               sql))))))))))

(ert-deftest clutch-test-result-filter-page-count-export-real-sqlite-workflow ()
  "Public result commands preserve one filtered SQLite workflow end to end."
  (skip-unless (sqlite-available-p))
  (let* ((conn (clutch-db-sqlite-connect '(:database ":memory:")))
         (source (generate-new-buffer " *clutch-result-workflow-source*"))
         (clutch-result-max-rows 2)
         (clutch--execution-refresh-timer nil)
         (kill-ring nil)
         (kill-ring-yank-pointer nil)
         result)
    (unwind-protect
        (save-window-excursion
          (clutch-db-query
           conn
           "CREATE TABLE metrics (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)")
          (clutch-db-query
           conn
           (concat "INSERT INTO metrics (id, name, score) VALUES "
                   "(1, 'one', 10), (2, 'two', 20), (3, 'three', 30), "
                   "(4, 'four', 40), (5, 'five', 50)"))
          (set-window-buffer (selected-window) source)
          (with-current-buffer source
            (clutch-mode)
            (setq-local clutch-connection conn
                        clutch--connection-params
                        '(:backend sqlite :database ":memory:"))
            (insert "SELECT name, score FROM metrics ORDER BY id")
            (clutch-execute-buffer)
            (setq result clutch--last-result-buffer))
          (should (buffer-live-p result))
          (set-window-buffer (selected-window) result)
          (with-current-buffer result
            (should (derived-mode-p 'clutch-result-mode))
            (should (memq 'clutch--header-line-display
                          (flatten-tree header-line-format)))
            (should (equal (clutch--column-names-for-indices
                            (clutch--visible-columns))
                           '("name" "score")))
            (should (equal (plist-get clutch--row-identity :indices) '(2)))
            (should (equal (mapcar (lambda (row) (cl-subseq row 0 2))
                                   clutch--result-rows)
                           '(("one" 10) ("two" 20))))
            (dolist (pattern '("two" "missing" ""))
              (cl-letf (((symbol-function 'read-string)
                         (lambda (&rest _) pattern)))
                (call-interactively (key-binding (kbd "/"))))
              (should (string-match-p (regexp-quote "1-2 of 3+ rows")
                                      clutch--footer-base-string))
              (should (eq (and clutch--page-has-more t) t))
              (when (equal pattern "two")
                (should (string-match-p
                         "1/2 page matches" (clutch--footer-mode-line-display))))
              (when (equal pattern "missing")
                (should (string-match-p "No matches on this page" (buffer-string))))
              (when (equal pattern "two")
                (clutch-result-copy-tsv)
                (should (string-match-p "two" (current-kill 0 t)))))
            (cl-letf (((symbol-function 'completing-read)
                       (lambda (&rest _args) "score"))
                      ((symbol-function 'read-string)
                       (lambda (&rest _args) "> 20")))
              (clutch-result-apply-filter)
              (should (memq 'clutch--header-line-display
                            (flatten-tree header-line-format)))
              (should (equal clutch--where-filter "\"score\" > 20"))
              (should (string-match-p "WHERE \"score\" > 20"
                                      clutch--last-query))
              (should (equal (plist-get clutch--row-identity :indices) '(2)))
              (should (equal clutch--result-rows
                             '(("three" 30 3) ("four" 40 4))))
              (clutch-result-count-total)
              (should (= clutch--page-total-rows 3))
              (clutch-result-next-page)
              (should (= clutch--page-current 1))
              (should-not clutch--page-has-more)
              (should (equal (plist-get clutch--row-identity :indices) '(2)))
              (should (equal clutch--result-rows '(("five" 50 5))))
              (cl-letf (((symbol-function 'read-string)
                         (lambda (&rest _) "not-a-row")))
                (call-interactively #'clutch-result-filter))
              (should-not (clutch--result-display-rows))
              (let ((suffix
                     (clutch-test--transient-suffix-for-key
                      'clutch-result-export "t")))
                (should suffix)
                (cl-letf (((symbol-function 'transient-args)
                           (lambda (_prefix) nil)))
                  (funcall (oref suffix command))))
              (let ((tsv (current-kill 0 t)))
                (should (equal tsv
                               "name\tscore\nthree\t30\nfour\t40\nfive\t50\n"))
                (should-not
                 (string-match-p "one\\|two\\|clutch__rid\\|id\t" tsv))))))
      (clutch--execution-refresh-stop)
      (when (buffer-live-p result)
        (kill-buffer result))
      (when (buffer-live-p source)
        (kill-buffer source))
      (when (clutch-db-live-p conn)
        (clutch-db-disconnect conn))
      (should-not clutch--execution-refresh-timer))))

(ert-deftest clutch-test-cte-result-rewrites-real-sqlite-workflow ()
  "A simple query over a CTE should count, sort and filter on the server."
  (skip-unless (sqlite-available-p))
  (let* ((conn (clutch-db-sqlite-connect '(:database ":memory:")))
         (source (generate-new-buffer " *clutch-cte-workflow-source*"))
         (clutch-result-max-rows 2)
         (clutch--execution-refresh-timer nil)
         result)
    (unwind-protect
        (save-window-excursion
          (clutch-db-query
           conn
           "CREATE TABLE metrics (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)")
          (clutch-db-query
           conn
           (concat "INSERT INTO metrics (id, name, score) VALUES "
                   "(1, 'one', 10), (2, 'two', 20), (3, 'three', 30), "
                   "(4, 'four', 40), (5, 'five', 50)"))
          (set-window-buffer (selected-window) source)
          (with-current-buffer source
            (clutch-mode)
            (setq-local clutch-connection conn
                        clutch--connection-params
                        '(:backend sqlite :database ":memory:"))
            (insert "WITH m AS (SELECT name, score FROM metrics) SELECT name, score FROM m ORDER BY score")
            (clutch-execute-buffer)
            (setq result clutch--last-result-buffer))
          (set-window-buffer (selected-window) result)
          (with-current-buffer result
            (should clutch--result-server-rewritable)
            (should (equal clutch--result-source-table "metrics"))
            (should (equal clutch--result-rows '(("one" 10 1) ("two" 20 2))))
            (clutch-result-count-total)
            (should (= clutch--page-total-rows 5))
            (clutch-result--sort "score" t)
            (should (equal clutch--result-rows '(("five" 50 5) ("four" 40 4))))
            (cl-letf (((symbol-function 'completing-read)
                       (lambda (&rest _args) "score"))
                      ((symbol-function 'read-string)
                       (lambda (&rest _args) "< 40")))
              (clutch-result-apply-filter))
            (should (string-prefix-p "WITH m AS" clutch--last-query))
            (clutch-result-count-total)
            (should (= clutch--page-total-rows 3))))
      (clutch--execution-refresh-stop)
      (when (buffer-live-p result)
        (kill-buffer result))
      (when (buffer-live-p source)
        (kill-buffer source))
      (when (clutch-db-live-p conn)
        (clutch-db-disconnect conn)))))

(ert-deftest clutch-test-cte-result-edits-base-table-real-sqlite-workflow ()
  "Edits of a CTE result should reach the one base table row they show."
  (skip-unless (sqlite-available-p))
  (let* ((conn (clutch-db-sqlite-connect '(:database ":memory:")))
         (source (generate-new-buffer " *clutch-cte-edit-source*"))
         (clutch--execution-refresh-timer nil)
         result)
    (cl-flet ((table-rows ()
                (clutch-db-result-rows
                 (clutch-db-query
                  conn "SELECT id, name, team FROM people ORDER BY id")))
              (edit-first-name (value)
                (let ((row (car clutch--result-rows)))
                  (clutch-result--apply-edit
                   0 0 value
                   (list :identity (clutch-db-row-identity-values
                                    row clutch--row-identity)
                         :original (car row)
                         :original-state (cons nil (car row)))))
                (cl-letf (((symbol-function 'yes-or-no-p)
                           (lambda (&rest _) t)))
                  (clutch-result-submit))))
      (unwind-protect
          (save-window-excursion
            (clutch-db-query
             conn "CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT, team TEXT)")
            (clutch-db-query
             conn (concat "INSERT INTO people (id, name, team) VALUES "
                          "(1, 'alpha', 'a'), (2, 'alpha', 'b'), (3, 'beta', 'b')"))
            (set-window-buffer (selected-window) source)
            (with-current-buffer source
              (clutch-mode)
              (setq-local clutch-connection conn
                          clutch--connection-params
                          '(:backend sqlite :database ":memory:"))
              ;; The key is not projected, and names repeat across rows.
              (insert "WITH c (n, t) AS (SELECT name, team FROM people) SELECT n, t FROM c ORDER BY t, n")
              (clutch-execute-buffer)
              (setq result clutch--last-result-buffer))
            (set-window-buffer (selected-window) result)
            (with-current-buffer result
              (should (equal clutch--result-source-table "people"))
              (should (equal (clutch--insert-target-table) "people"))
              (edit-first-name "gamma")
              (should (equal (table-rows)
                             '((1 "gamma" "a") (2 "alpha" "b") (3 "beta" "b"))))
              (cl-letf (((symbol-function 'completing-read)
                         (lambda (&rest _args) "t"))
                        ((symbol-function 'read-string)
                         (lambda (&rest _args) "= 'b'")))
                (clutch-result-apply-filter))
              (should (plist-get clutch--row-identity :indices))
              (should (equal (mapcar (lambda (row) (cl-subseq row 0 2))
                                     clutch--result-rows)
                             '(("alpha" "b") ("beta" "b"))))
              (edit-first-name "delta")
              (should (equal (table-rows)
                             '((1 "gamma" "a") (2 "delta" "b") (3 "beta" "b"))))))
        (clutch--execution-refresh-stop)
        (when (buffer-live-p result)
          (kill-buffer result))
        (when (buffer-live-p source)
          (kill-buffer source))
        (when (clutch-db-live-p conn)
          (clutch-db-disconnect conn))))))

(ert-deftest clutch-test-cte-result-deletes-and-inserts-real-sqlite-workflow ()
  "Deletes and inserts through a CTE result should reach its base table."
  (skip-unless (sqlite-available-p))
  (let* ((conn (clutch-db-sqlite-connect '(:database ":memory:")))
         (source (generate-new-buffer " *clutch-cte-delete-source*"))
         (clutch--execution-refresh-timer nil)
         result)
    (cl-flet ((table-rows ()
                (clutch-db-result-rows
                 (clutch-db-query
                  conn "SELECT id, name, team FROM people ORDER BY id"))))
      (unwind-protect
          (save-window-excursion
            (clutch-db-query
             conn "CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT, team TEXT)")
            (clutch-db-query
             conn "INSERT INTO people (id, name, team) VALUES (1, 'alpha', 'a'), (2, 'alpha', 'b')")
            (set-window-buffer (selected-window) source)
            (with-current-buffer source
              (clutch-mode)
              (setq-local clutch-connection conn
                          clutch--connection-params
                          '(:backend sqlite :database ":memory:"))
              (insert "WITH c (n, t) AS (SELECT name, team FROM people) SELECT n, t FROM c ORDER BY t")
              (clutch-execute-buffer)
              (setq result clutch--last-result-buffer))
            (set-window-buffer (selected-window) result)
            (with-current-buffer result
              (cl-letf (((symbol-function 'yes-or-no-p) (lambda (&rest _) t)))
                ;; The second row shown shares its name with the first.
                (goto-char (aref clutch--row-start-positions 1))
                (clutch-result-delete-rows)
                (clutch-result-submit)
                (should (equal (table-rows) '((1 "alpha" "a"))))
                (setq-local clutch--pending-inserts
                            '((("id" . "3") ("name" . "omega") ("team" . "c"))))
                (clutch-result-submit)
                (should (equal (table-rows)
                               '((1 "alpha" "a") (3 "omega" "c")))))))
        (clutch--execution-refresh-stop)
        (when (buffer-live-p result)
          (kill-buffer result))
        (when (buffer-live-p source)
          (kill-buffer source))
        (when (clutch-db-live-p conn)
          (clutch-db-disconnect conn))))))

(ert-deftest clutch-test-cte-result-edits-shown-row-past-identity-like-columns ()
  "An edit through a CTE should change the row shown whatever columns exist.
Outer SELECTs pass the identity on by name, so a column named like it must
not take its place, whether the query names it, the table has it, the
table sits in another schema, or the column is generated."
  (skip-unless (sqlite-available-p))
  (dolist (case
           '(("query alias"
              ("CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)"
               "INSERT INTO people VALUES (1, 'alpha'), (2, 'beta')")
              "WITH c AS (SELECT name, 2 AS clutch__rid_0 FROM people WHERE id = 1) SELECT name FROM c"
              "people")
             ("table column"
              ("CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT, clutch__rid_0 INTEGER)"
               "INSERT INTO people VALUES (1, 'alpha', 2), (2, 'beta', 1)")
              "WITH c AS (SELECT * FROM people WHERE id = 1) SELECT name FROM c"
              "people")
             ("table in another schema"
              ("CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)"
               "INSERT INTO people VALUES (1, 'alpha'), (2, 'beta')"
               "ATTACH DATABASE ':memory:' AS aux"
               "CREATE TABLE aux.people (id INTEGER PRIMARY KEY, name TEXT, clutch__rid_0 INTEGER)"
               "INSERT INTO aux.people VALUES (1, 'alpha', 2), (2, 'beta', 1)")
              "WITH c AS (SELECT * FROM aux.people WHERE id = 1) SELECT name FROM c"
              "aux.people")
             ("generated column"
              ("CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT, clutch__rid_0 INTEGER GENERATED ALWAYS AS (3 - id) VIRTUAL)"
               "INSERT INTO people (id, name) VALUES (1, 'alpha'), (2, 'beta')")
              "WITH c AS (SELECT * FROM people WHERE id = 1) SELECT name FROM c"
              "people")))
    (pcase-let ((`(,label ,setup ,sql ,table) case))
      (ert-info (label)
        (let* ((conn (clutch-db-sqlite-connect '(:database ":memory:")))
               (source (generate-new-buffer " *clutch-cte-identity-name-source*"))
               (clutch--execution-refresh-timer nil)
               result)
          (unwind-protect
              (save-window-excursion
                (dolist (statement setup)
                  (clutch-db-query conn statement))
                (set-window-buffer (selected-window) source)
                (with-current-buffer source
                  (clutch-mode)
                  (setq-local clutch-connection conn
                              clutch--connection-params
                              '(:backend sqlite :database ":memory:"))
                  (insert sql)
                  (clutch-execute-buffer)
                  (setq result clutch--last-result-buffer))
                (set-window-buffer (selected-window) result)
                (with-current-buffer result
                  (should (equal (mapcar #'car clutch--result-rows) '("alpha")))
                  (let ((row (car clutch--result-rows)))
                    (clutch-result--apply-edit
                     0 0 "edited"
                     (list :identity (clutch-db-row-identity-values
                                      row clutch--row-identity)
                           :original (car row)
                           :original-state (cons nil (car row)))))
                  (cl-letf (((symbol-function 'yes-or-no-p)
                             (lambda (&rest _) t)))
                    (clutch-result-submit)))
                (should (equal (clutch-db-result-rows
                                (clutch-db-query
                                 conn (format "SELECT id, name FROM %s ORDER BY id"
                                              table)))
                               '((1 "edited") (2 "beta")))))
            (clutch--execution-refresh-stop)
            (when (buffer-live-p result)
              (kill-buffer result))
            (when (buffer-live-p source)
              (kill-buffer source))
            (when (clutch-db-live-p conn)
              (clutch-db-disconnect conn))))))))

(ert-deftest clutch-test-insert-export-names-source-columns-real-sqlite ()
  "INSERT export should name the table's columns, not the result's aliases."
  (skip-unless (sqlite-available-p))
  (let* ((conn (clutch-db-sqlite-connect '(:database ":memory:")))
         (source (generate-new-buffer " *clutch-insert-export-source*"))
         (clutch--execution-refresh-timer nil))
    (unwind-protect
        (save-window-excursion
          (clutch-db-query conn "CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)")
          (clutch-db-query conn "INSERT INTO people (id, name) VALUES (1, 'alpha')")
          (set-window-buffer (selected-window) source)
          (dolist (sql '("SELECT id AS k, name AS v FROM people"
                         "WITH c (k, v) AS (SELECT id, name FROM people) SELECT * FROM c"))
            (ert-info (sql)
              (let (result)
                (with-current-buffer source
                  (clutch-mode)
                  (setq-local clutch-connection conn
                              clutch--connection-params
                              '(:backend sqlite :database ":memory:"))
                  (erase-buffer)
                  (insert sql)
                  (clutch-execute-buffer)
                  (setq result clutch--last-result-buffer))
                (with-current-buffer result
                  (should (equal (clutch-result--build-insert-statements-for-rows
                                  clutch--result-rows (clutch--visible-columns)
                                  (clutch--insert-target-table))
                                 '("INSERT INTO \"people\" (\"id\", \"name\") VALUES (1, 'alpha');")))
                  (kill-buffer result))))))
      (clutch--execution-refresh-stop)
      (when (buffer-live-p source)
        (kill-buffer source))
      (when (clutch-db-live-p conn)
        (clutch-db-disconnect conn)))))

(ert-deftest clutch-test-column-sizing-bounds-long-value-work ()
  "Column sizing should stop measuring once the display cap is reached."
  (let ((measure (symbol-function 'string-width))
        (long (make-string 1048576 ?x))
        (clutch-column-width-max 30))
    (cl-letf (((symbol-function 'string-width)
               (lambda (text &rest args)
                 (should (< (length text) 128))
                 (apply measure text args))))
      (should (equal (clutch--compute-column-widths
                      '("value") (make-list 50 (list long))
                      '((:name "value" :type-category text)))
                     [30])))))

(ert-deftest clutch-test-capped-width-preserves-display-width ()
  "Capped measurement should preserve Unicode, controls and compositions."
  (with-temp-buffer
    (dolist (text (list "" "abc" "中文" "á🙂" "a\tb\nc"
                        (concat (make-string 80 ?\u0301) "x")
                        (compose-string (make-string 80 ?x) 0 80 ?z)))
      (dotimes (limit 40)
        (should (= (clutch--capped-string-width text limit)
                   (min limit (string-width text))))))
    (setq-local buffer-display-table (make-display-table))
    (aset buffer-display-table ?x [?a ?b ?c])
    (should (= (clutch--capped-string-width "xxxx" 8) 8))))

(ert-deftest clutch-test-cell-prefix-preserves-isolated-mark-width ()
  "Prefix scanning must honor the buffer's isolated combining-mark width."
  (let ((measure (symbol-function 'string-width)))
    (dolist (isolated-width '(0 1))
      (cl-letf (((symbol-function 'string-width)
                 (lambda (text &rest args)
                   (if (equal text "́") isolated-width
                     (apply measure text args)))))
        (should (equal (clutch--cell-visible-prefix "á" 1)
                       (if (zerop isolated-width) '("á") '("…" . t))))))))

(defun clutch-test--check-file-export (suffix &optional target-suffix)
  "Check encoding, batching and destination preservation with SUFFIX.
When TARGET-SUFFIX is non-nil, export through a link to that suffix."
  (let* ((dir (make-temp-file "clutch-export-test-" t))
         (path (expand-file-name (concat "out.csv" suffix) dir))
         (target (when target-suffix (concat "target.csv" target-suffix)))
         (rows '((1 "中文,a") (2 "x\ny") (3 "z\uFEFF😀"))))
    (unwind-protect
        (with-temp-buffer
          (auto-compression-mode 1)
          (when target (make-symbolic-link target path))
          (setq-local clutch--result-columns '("id" "name"))
          (cl-letf (((symbol-function 'read-file-name) (lambda (&rest _) path))
                    ((symbol-function 'clutch-result--collect-all-export-rows)
                     (lambda (_on-rows) (ert-fail "File export collected all rows"))))
            (dolist (coding '(utf-8-with-signature utf-8 gb18030
                                                   utf-16 utf-16-dos utf-16le utf-16be
                                                   utf-16le-with-signature
                                                   utf-16be-with-signature-dos))
              (cl-letf (((symbol-function 'clutch--read-delimited-export-coding-system)
                         (lambda (_) coding))
                        ((symbol-function 'clutch-result--map-export-batches)
                         (lambda (function done)
                           (funcall function (seq-take rows 2))
                           (funcall function (last rows))
                           (funcall done nil))))
                (clutch--export-result 'csv 'file))
              (unless (string-empty-p suffix)
                (should (equal (with-temp-buffer
                                 (set-buffer-multibyte nil)
                                 (insert-file-contents-literally path nil 0 2)
                                 (buffer-string))
                               (unibyte-string #x1f #x8b))))
              (should (equal (with-temp-buffer
                               (set-buffer-multibyte nil)
                               (let ((coding-system-for-read 'no-conversion))
                                 (insert-file-contents path))
                               (buffer-string))
                             (encode-coding-string (clutch--export-csv-content rows)
                                                   coding))))
            (let ((before (with-temp-buffer
                            (insert-file-contents-literally path)
                            (buffer-string))))
              ;; A failure reported to DONE, as a failed page is, returns.
              (dolist (exit '(error quit incomplete failed))
                (cl-letf (((symbol-function 'clutch--read-delimited-export-coding-system)
                           (lambda (_) 'utf-8))
                          ((symbol-function 'clutch-result--map-export-batches)
                           (lambda (function done)
                             (funcall function (list (car rows)))
                             (pcase exit
                               ('error (error "Second page failed"))
                               ('quit (signal 'quit nil))
                               ('incomplete
                                (funcall function
                                         (list (list 2 (make-clutch-db-value-preview
                                                        :type 'clob :length 100
                                                        :text "preview")))))
                               ('failed (funcall done '(error "Second page failed")))))))
                  (should (eq (condition-case err
                                  (progn (clutch--export-result 'csv 'file) 'failed)
                                (error (car err)) (quit 'quit))
                              (pcase exit ('incomplete 'user-error) (_ exit))))
                  (should (equal before (with-temp-buffer
                                          (insert-file-contents-literally path)
                                          (buffer-string)))))))
            (when target (should (equal (file-symlink-p path) target)))
            (should (equal (directory-files dir nil "\\`[^.]")
                           (append (list (file-name-nondirectory path))
                                   (when target (list target)))))))
      (delete-directory dir t))))

(ert-deftest clutch-test-streamed-export-preserves-content-and-file ()
  "Check ordinary-file encoding, batching and failure preservation."
  (clutch-test--check-file-export ""))

(ert-deftest clutch-test-file-export-preserves-compression ()
  "Check gzip encoding, batching and failure preservation."
  (skip-unless (executable-find "gzip"))
  (clutch-test--check-file-export ".gz"))

(ert-deftest clutch-test-export-symlink-keeps-selected-filename-handler ()
  "The selected name controls compression even across differently named links."
  (skip-unless (executable-find "gzip"))
  (clutch-test--check-file-export ".gz" "")
  (clutch-test--check-file-export "" ".gz"))

(ert-deftest clutch-test-export-handler-writes-once-and-preserves-failures ()
  "A non-appending handler sees complete bytes and cannot damage the target."
  (let* ((dir (make-temp-file "clutch-export-handler-" t))
         (path (expand-file-name "out.clutch-test" dir))
         (rows '(("中文,a") ("last")))
         (calls 0)
         outcome handler)
    (setq handler
          (lambda (operation &rest args)
            (let ((inhibit-file-name-handlers
                   (cons handler (and (eq inhibit-file-name-operation operation)
                                      inhibit-file-name-handlers)))
                  (inhibit-file-name-operation operation))
              (if (not (eq operation 'write-region))
                  (apply operation args)
                (cl-incf calls)
                (should-not (nth 3 args))
                (let ((bytes (if (stringp (car args)) (car args)
                               (buffer-substring-no-properties
                                (car args) (cadr args)))))
                  (write-region (concat "encoded:" bytes) nil
                                (nth 2 args) nil 'silent))
                (pcase outcome
                  ('file-error (signal 'file-error '("Transform failed")))
                  ('quit (signal 'quit nil)))))))
    (unwind-protect
        (with-temp-buffer
          (setq-local clutch--result-columns '("value"))
          (dolist (result '(success file-error quit))
            (setq outcome result calls 0)
            (write-region "old\n" nil path nil 'silent)
            (set-file-modes path #o640)
            (let ((file-name-handler-alist
                   (cons (cons "\\.clutch-test\\'" handler)
                         file-name-handler-alist)))
              (cl-letf (((symbol-function 'clutch-result--map-export-batches)
                         (lambda (function done)
                           (funcall function (list (car rows)))
                           (funcall function (cdr rows))
                           (funcall done nil))))
                (should (eq (condition-case err
                                (progn
                                  (clutch-result--write-export-file
                                   'csv (cdr (assq 'csv clutch--result-export-kinds))
                                   path #'ignore :coding 'utf-8-with-signature)
                                  'success)
                              (file-error (car err))
                              (quit 'quit))
                            result))))
            (should (= calls 1))
            (should (equal (with-temp-buffer
                             (set-buffer-multibyte nil)
                             (insert-file-contents-literally path)
                             (buffer-string))
                           (if (eq result 'success)
                               (concat "encoded:"
                                       (encode-coding-string
                                        (clutch--export-csv-content rows)
                                        'utf-8-with-signature))
                             "old\n")))
            (should (= (file-modes path) #o640))
            (should (equal (directory-files dir nil "\\`[^.]")
                           '("out.clutch-test")))))
      (delete-directory dir t))))

(ert-deftest clutch-test-streamed-export-preserves-symlinks ()
  "Export follows relative link chains and preserves their targets on failure."
  (dolist (existing '(t nil))
    (dolist (outcome '(success error quit incomplete))
      (let* ((dir (make-temp-file "clutch-export-link-" t))
             (target-dir (expand-file-name "data" dir))
             (target (expand-file-name "real.csv" target-dir))
             (link (expand-file-name "export.csv" dir))
             (middle (expand-file-name "current.csv" dir)))
        (unwind-protect
            (with-temp-buffer
              (make-directory target-dir)
              (when existing
                (write-region "old\n" nil target nil 'silent)
                (set-file-modes target #o640))
              (make-symbolic-link "data/real.csv" middle)
              (make-symbolic-link "current.csv" link)
              (setq-local clutch--result-columns '("value"))
              (cl-letf (((symbol-function 'read-file-name) (lambda (&rest _) link))
                        ((symbol-function 'clutch--read-delimited-export-coding-system)
                         (lambda (_) 'utf-8))
                        ((symbol-function 'clutch-result--map-export-batches)
                         (lambda (function done)
                           (funcall function '(("new")))
                           (pcase outcome
                             ('error (error "Second page failed"))
                             ('quit (signal 'quit nil))
                             ('incomplete
                              (funcall function
                                       (list (list (make-clutch-db-value-preview
                                                    :type 'clob :length 1000
                                                    :text "preview")))))
                             (_ (funcall function '(("last")))
                                (funcall done nil))))))
                (should (eq (condition-case err
                                (progn (clutch--export-result 'csv 'file) 'success)
                              (error (car err))
                              (quit 'quit))
                            (if (eq outcome 'incomplete) 'user-error outcome))))
              (should (equal (file-symlink-p link) "current.csv"))
              (should (equal (file-symlink-p middle) "data/real.csv"))
              (if (or existing (eq outcome 'success))
                  (should (equal (with-temp-buffer
                                   (insert-file-contents target)
                                   (buffer-string))
                                 (if (eq outcome 'success)
                                     "value\nnew\nlast\n"
                                   "old\n")))
                (should-not (file-exists-p target)))
              (when existing
                (should (= (file-modes target) #o640)))
              (should (equal (directory-files dir nil "\\`[^.]")
                             '("current.csv" "data" "export.csv")))
              (should (equal (directory-files target-dir nil "\\`[^.]")
                             (when (or existing (eq outcome 'success))
                               '("real.csv")))))
          (delete-directory dir t))))))

(ert-deftest clutch-test-file-export-pages-through-sqlite ()
  "Paged file output should equal complete formatting for every SQL format."
  (require 'clutch-db-sqlite)
  (skip-unless (sqlite-available-p))
  (let ((conn (clutch-db-connect 'sqlite '(:database ":memory:")))
        (path (make-temp-file "clutch-export-pages-")))
    (unwind-protect
        (progn
          (clutch-db-query conn "CREATE TABLE items(id INTEGER PRIMARY KEY, body TEXT)")
          (clutch-db-query conn "INSERT INTO items VALUES (1, '中文,a'), (2, 'x'), (3, NULL), (4, ''), (5, 'z')")
          (clutch-test--with-result-state
              (:connection conn :base-query "SELECT id, body FROM items ORDER BY id"
               :last-query "SELECT id, body FROM items ORDER BY id"
               :server-pageable t :columns '("id" "body")
               :column-defs '((:name "id" :type-category numeric)
                              (:name "body" :type-category text)))
            (setq-local clutch--result-source-table "items"
                        clutch--row-identity
                        (clutch-test--primary-row-identity "items" '("id") '(0)))
            (let ((clutch-result-max-rows 2)
                  (rows (clutch-db-result-rows
                         (clutch-db-query conn clutch--base-query))))
              (dolist (kind '(csv tsv insert update))
                (dolist (omit-header '(nil t))
                  (let* ((spec (cdr (assq kind clutch--result-export-kinds)))
                         (content (plist-get spec :content))
                         (expected (if (memq kind '(csv tsv))
                                       (funcall content rows omit-header)
                                     (funcall content rows)))
                         (calls 0)
                         (format-batch (symbol-function content)))
                    (cl-letf (((symbol-function 'read-file-name) (lambda (&rest _) path))
                              ((symbol-function 'clutch--read-delimited-export-coding-system)
                               (lambda (_) 'utf-8))
                              ((symbol-function content)
                               (lambda (batch &rest args)
                                 (should (<= (length batch) 2))
                                 (cl-incf calls)
                                 (apply format-batch batch args))))
                      (clutch--export-result kind 'file omit-header))
                    (should (= calls 3))
                    (should (equal expected (with-temp-buffer
                                              (insert-file-contents path)
                                              (buffer-string))))))))))
      (delete-file path)
      (clutch-db-disconnect conn))))

(ert-deftest clutch-test-file-export-replaces-file-once-all-pages-arrive ()
  "A file export should fetch its pages without blocking.
The file changes only once every page has arrived.  A failed page, a
cancel that meets a page as it arrives, a quit while a page is written,
or a killed result buffer, leaves it and no temporary file behind."
  (dolist (outcome '(success page-error cancelled quit killed))
    (ert-info ((symbol-name outcome))
      (let* ((dir (make-temp-file "clutch-export-async-" t))
             (path (expand-file-name "out.csv" dir))
             (result-buffer (generate-new-buffer " *clutch-export-async*"))
             exported)
        (unwind-protect
            (clutch-test--with-async-statements finishes
              (write-region "old\n" nil path nil 'silent)
              (with-current-buffer result-buffer
                (clutch-test--init-result-state
                 (list :columns '("id")
                       :rows '((1) (2))
                       :connection 'async-conn
                       :base-query "SELECT id FROM t"
                       :server-pageable t
                       :result-max-rows 2))
                (cl-letf (((symbol-function 'clutch--ensure-connection) #'ignore)
                          ((symbol-function 'clutch-db-build-paged-sql)
                           (lambda (_conn _sql page-num &rest _)
                             (format "SELECT id FROM t PAGE %d" page-num)))
                          ((symbol-function 'message) #'ignore))
                  (clutch-result--write-export-file
                   'csv (cdr (assq 'csv clutch--result-export-kinds)) path
                   (lambda (row-count) (setq exported row-count))
                   :coding 'utf-8)
                  (should (equal (mapcar #'car finishes) '("SELECT id FROM t PAGE 0")))
                  (funcall (cdar finishes) (make-clutch-db-result :rows '((1) (2))) nil)
                  (ert-run-idle-timers)
                  (should (equal (car (car finishes)) "SELECT id FROM t PAGE 1"))
                  (should (equal (with-temp-buffer (insert-file-contents path)
                                                   (buffer-string))
                                 "old\n"))
                  (pcase outcome
                    ('success
                     (funcall (cdar finishes) (make-clutch-db-result :rows '((3))) nil))
                    ('page-error
                     (funcall (cdar finishes) nil '(clutch-db-error "connection reset")))
                    ('cancelled
                     (cl-letf (((symbol-function 'clutch-db-interrupt-query)
                                (lambda (_conn) t)))
                       (clutch-cancel-query-or-quit))
                     (funcall (cdar finishes) (make-clutch-db-result :rows '((3))) nil))
                    ('quit
                     (let ((write-region (symbol-function 'write-region)))
                       (cl-letf (((symbol-function 'write-region)
                                  (lambda (&rest args)
                                    (if (string-match-p "out\\.csv\\.clutch-"
                                                        (nth 2 args))
                                        (signal 'quit nil)
                                      (apply write-region args)))))
                         (funcall (cdar finishes)
                                  (make-clutch-db-result :rows '((3))) nil)
                         ;; ERT does not fail a test that quits.
                         (should-not (condition-case nil
                                         (progn (ert-run-idle-timers) nil)
                                       (quit t))))))
                    ('killed
                     (let ((finish (cdar finishes)))
                       (kill-buffer result-buffer)
                       (funcall finish (make-clutch-db-result :rows '((3))) nil))))
                  (ert-run-idle-timers)))
              (should-not (gethash 'async-conn clutch--running-queries))
              (should-not (clutch-db--foreground-busy-p 'async-conn))
              (should (equal exported (and (eq outcome 'success) 3)))
              (should (equal (with-temp-buffer (insert-file-contents path)
                                               (buffer-string))
                             (if (eq outcome 'success) "id\n1\n2\n3\n" "old\n")))
              (should (equal (directory-files dir nil "\\`[^.]") '("out.csv"))))
          (when (buffer-live-p result-buffer)
            (kill-buffer result-buffer))
          (delete-directory dir t))))))

(ert-deftest clutch-test-export-ends-once-when-its-failure-cannot-be-shown ()
  "An export should end once even when showing a page's failure fails.
Its connection is released and its completion runs once, as a batch ends
when its continuation fails."
  (with-temp-buffer
    (clutch-test--with-async-statements finishes
      (clutch-test--init-result-state
       (list :columns '("id") :rows '((1)) :connection 'async-conn
             :base-query "SELECT id FROM t" :server-pageable t
             :result-max-rows 2))
      (let (completions)
        (cl-letf (((symbol-function 'clutch-db-build-paged-sql)
                   (lambda (_conn _sql page-num &rest _)
                     (format "SELECT id FROM t PAGE %d" page-num)))
                  ((symbol-function 'clutch--present-statement-outcome)
                   (lambda (&rest _) (error "Display failed")))
                  ((symbol-function 'message) #'ignore))
          (clutch-result--export-pages "SELECT id FROM t" "SELECT id FROM t" 2
                                       #'ignore
                                       (lambda (err) (push err completions)))
          (funcall (cdar finishes) nil '(clutch-db-error "connection reset"))
          (ignore-errors (ert-run-idle-timers))
          (should (= (length completions) 1))
          (should-not (clutch-db--foreground-busy-p 'async-conn)))))))

(ert-deftest clutch-test-export-stops-when-its-result-loses-its-connection ()
  "An export whose result lost its connection should stop without showing the page.
The disconnect that cleared the result's connection also closed it, and the
page in flight then failed; showing that failure would put an error page in
a buffer that holds no connection, or holds another one."
  (clutch-test--with-result-state
      (:columns '("id") :rows '((1)) :connection 'async-conn
       :base-query "SELECT id FROM t" :server-pageable t :result-max-rows 2)
    (clutch-test--with-async-statements finishes
      (let (shown completions)
        (cl-letf (((symbol-function 'clutch-db-build-paged-sql)
                   (lambda (_conn _sql page-num &rest _)
                     (format "SELECT id FROM t PAGE %d" page-num)))
                  ((symbol-function 'clutch--connection-alive-p)
                   (lambda (conn) (not (eq conn 'async-conn))))
                  ((symbol-function 'clutch--show-execution-error)
                   (lambda (&rest _) (setq shown t) "failed"))
                  ((symbol-function 'message) #'ignore))
          (clutch-result--export-pages "SELECT id FROM t" "SELECT id FROM t" 2
                                       #'ignore
                                       (lambda (err) (push err completions)))
          ;; A disconnect elsewhere clears the result's connection.
          (with-temp-buffer
            (clutch--invalidate-derived-buffers 'async-conn))
          (funcall (cdar finishes) nil
                   '(clutch-db-error
                     "Disconnected while the statement ran; its outcome is unknown"))
          (ert-run-idle-timers)
          (should-not shown)
          (should (= (length completions) 1))
          (should (string-match-p "connection changed"
                                  (error-message-string (car completions))))
          (should-not (clutch-db--foreground-busy-p 'async-conn)))))))

(ert-deftest clutch-test-export-stops-when-its-result-moves-during-a-synchronous-page ()
  "An export should stop when its result loses its connection during a page.
A backend that waits on the network synchronously runs timers while a page
runs, and a disconnect from one of them clears the result's connection."
  (require 'clutch-db-sqlite)
  (skip-unless (sqlite-available-p))
  (let ((conn (clutch-db-connect 'sqlite '(:database ":memory:")))
        (run-db-query (symbol-function 'clutch--run-db-query))
        pages completions)
    (unwind-protect
        (progn
          (clutch-db-query conn "CREATE TABLE n(i INTEGER PRIMARY KEY)")
          (clutch-db-query conn "INSERT INTO n VALUES (1), (2), (3), (4)")
          (clutch-test--with-result-state
              (:connection conn :base-query "SELECT i FROM n ORDER BY i"
               :last-query "SELECT i FROM n ORDER BY i"
               :server-pageable t :columns '("i"))
            (cl-letf (((symbol-function 'clutch--run-db-query)
                       (lambda (&rest args)
                         (push (nth 1 args) pages)
                         (prog1 (apply run-db-query args)
                           ;; A timer that runs while the page waits
                           ;; disconnects the result's connection.
                           (with-temp-buffer
                             (clutch--invalidate-derived-buffers conn)))))
                      ((symbol-function 'message) #'ignore))
              (clutch-result--export-pages
               "SELECT i FROM n ORDER BY i" "SELECT i FROM n ORDER BY i" 2
               #'ignore (lambda (err) (push err completions)))))
          (should (= (length pages) 1))
          (should (= (length completions) 1))
          (should (string-match-p "connection changed"
                                  (error-message-string (car completions)))))
      (clutch-db-disconnect conn))))

(ert-deftest clutch-test-file-export-of-many-pages-keeps-a-flat-stack ()
  "An export should fetch many synchronous pages without nesting them.
SQLite runs each page before its callback returns, which a recursive page
loop turns into a stack as deep as the pages are many."
  (require 'clutch-db-sqlite)
  (skip-unless (sqlite-available-p))
  (let ((conn (clutch-db-connect 'sqlite '(:database ":memory:")))
        (path (make-temp-file "clutch-export-many-"))
        exported)
    (unwind-protect
        (progn
          (clutch-db-query conn "CREATE TABLE n(i INTEGER PRIMARY KEY)")
          (clutch-db-query
           conn "WITH RECURSIVE c(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM c WHERE i < 800) INSERT INTO n SELECT i FROM c")
          (clutch-test--with-result-state
              (:connection conn :base-query "SELECT i FROM n ORDER BY i"
               :last-query "SELECT i FROM n ORDER BY i"
               :server-pageable t :columns '("i"))
            ;; Far fewer frames than pages: a page that nested the next
            ;; would run out of depth long before the last.
            (let ((clutch-result-max-rows 1)
                  (max-lisp-eval-depth 1000))
              (cl-letf (((symbol-function 'message) #'ignore))
                (clutch-result--write-export-file
                 'csv (cdr (assq 'csv clutch--result-export-kinds)) path
                 (lambda (row-count) (setq exported row-count))
                 :coding 'utf-8))))
          (should (eql exported 800)))
      (delete-file path)
      (clutch-db-disconnect conn))))

(ert-deftest clutch-test-collect-all-export-rows-contract ()
  "Export row collection should page, reuse local rows, and reconnect when needed."
  (dolist (case '(("plain limit"
                   "SELECT id FROM t LIMIT 2" nil ((1) (2)) 100)
                  ("sorted limit"
                   "SELECT id FROM t LIMIT 3" ("id" . "DESC")
                   ((3) (2) (1)) 2)))
    (pcase-let ((`(,label ,query ,order-by ,rows ,max-rows) case))
      (ert-info ((format "nonpageable: %s" label))
        (clutch-test--with-result-state
            (:base-query query
             :last-query query
             :order-by order-by
             :server-pageable nil
             :rows rows)
          (let ((clutch-result-max-rows max-rows)
                (queries 0)
                paginated)
            (cl-letf (((symbol-function 'clutch--connection-alive-p)
                       (lambda (_conn) t))
                      ((symbol-function 'clutch-db-build-paged-sql)
                       (lambda (&rest _args)
                         (setq paginated t)
                         "unexpected page query"))
                      ((symbol-function 'clutch-db-query)
                       (lambda (_conn _sql)
                         (cl-incf queries)
                         (make-clutch-db-result :rows rows))))
              (let (collected)
                (clutch-result--collect-all-export-rows
                 (lambda (all) (setq collected all)))
                (should (equal collected rows)))
              (should (= queries 0))
              (should-not paginated)))))))
  (ert-info ("reconnect before querying")
    (clutch-test--with-result-state
        (:connection 'stale-conn
         :base-query "SELECT id FROM t LIMIT 1"
         :last-query "SELECT id FROM t LIMIT 1"
         :server-pageable t)
      (let (ensured captured-conn)
        (cl-letf (((symbol-function 'clutch--ensure-connection)
                   (lambda ()
                     (setq ensured t)
                     (setq-local clutch-connection 'new-conn)))
                  ((symbol-function 'clutch-db-query)
                   (lambda (conn _sql)
                     (setq captured-conn conn)
                     (make-clutch-db-result :rows '((1))))))
          (let (collected)
            (clutch-result--collect-all-export-rows
             (lambda (all) (setq collected all)))
            (should (equal collected '((1)))))
          (should ensured)
          (should (eq captured-conn 'new-conn)))))))

(ert-deftest clutch-test-execute-select-detects-primary-key-before-first-render ()
  "Primary-key identity should be ready before the first result render."
  (let ((clutch--row-identity-cache (make-hash-table :test 'eq))
        (clutch--source-window (selected-window))
        (result-name "*clutch-test-result*")
        (captured-identity :unset))
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
                         (setq captured-identity clutch--row-identity)))
        (clutch-test--execute-and-present "SELECT * FROM users" 'fake-conn)
        (should (eq (plist-get captured-identity :kind) 'primary-key))
        (should (equal (plist-get captured-identity :source-indices) '(0)))
        (with-current-buffer result-name
          (should clutch--result-server-pageable)
          (should clutch--result-server-rewritable))))))

(ert-deftest clutch-test-execute-select-fetches-one-row-lookahead ()
  "Initial SELECT execution should trim lookahead rows before rendering."
  (let ((clutch--source-window (selected-window))
        (clutch--row-identity-cache (make-hash-table :test 'eq))
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
        (clutch-test--execute-and-present "SELECT id FROM users" 'fake-conn)
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
              (clutch--row-identity-cache (make-hash-table :test 'eq))
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
              (clutch-test--execute-and-present sql 'fake-conn)
              (should (equal captured-sql sql))
              (with-current-buffer result-name
                (should-not clutch--result-server-pageable)
                (should-not clutch--result-server-rewritable)
                (should-not clutch--page-has-more)
                (when (eq label 'complex-limit)
                  (should-not clutch--result-source-table))
                (when expected-rows
                  (should (equal clutch--result-rows expected-rows)))))))))))

(ert-deftest clutch-test-execute-simple-limit-select-retains-edit-source ()
  "A simple SELECT with LIMIT should retain its staged-edit source table."
  (let ((clutch--row-identity-cache (make-hash-table :test 'eq))
        (clutch--source-window (selected-window))
        (result-name "*clutch-test-result*")
        (sql "SELECT id, name FROM users LIMIT 10")
        captured-sql)
    (cl-letf (((symbol-function 'clutch-db-row-identity-candidates)
               (lambda (_conn _table)
                 (list (list :kind 'primary-key
                             :name "PRIMARY"
                             :columns '("id")))))
              ((symbol-function 'clutch-db-escape-identifier)
               (lambda (_conn id) (format "\"%s\"" id)))
              ((symbol-function 'clutch--run-db-query)
               (lambda (_conn query)
                 (setq captured-sql query)
                 (make-clutch-db-result
                  :columns '((:name "id")
                             (:name "name")
                             (:name "clutch__rid_0"))
                  :rows '((1 "alice" 1))))))
      (clutch-test--with-result-buffer (result-name)
        (clutch-test--execute-and-present sql 'fake-conn)
        (should (string-match-p "LIMIT 10\\'" captured-sql))
        (with-current-buffer result-name
          (should-not clutch--result-server-pageable)
          (should-not clutch--result-server-rewritable)
          (should (equal clutch--result-source-table "users"))
          (should (equal (clutch--result-source-table-or-user-error "edit cell")
                         "users"))
          (should (eq (plist-get clutch--row-identity :kind)
                      'primary-key)))))))

(ert-deftest clutch-test-execute-select-duplicate-labels-are-not-rewritable ()
  "Duplicate result labels should remain pageable but not relation-rewritable."
  (let ((clutch--source-window (selected-window))
        (clutch--row-identity-cache (make-hash-table :test 'eq))
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
        (clutch-test--execute-and-present sql 'fake-conn)
        (should (equal captured-build-sql sql))
        (with-current-buffer result-name
          (should clutch--result-server-pageable)
          (should-not clutch--result-server-rewritable))))))

(ert-deftest clutch-test-execute-select-honors-result-context-overrides ()
  "Internal filter SQL should keep the verified relation source capabilities."
  (let ((clutch--row-identity-cache (make-hash-table :test 'eq))
        (clutch--source-window (selected-window))
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
        (clutch-test--execute-and-present sql 'fake-conn result-context)
        (should (string-match-p "`id` AS `clutch__rid_0`"
                                captured-base-sql))))))

(ert-deftest clutch-test-execute-select-remembers-result-buffer-on-source-buffer ()
  "SELECT execution should attach the result buffer to the source buffer."
  (let ((source (generate-new-buffer " *clutch-source*"))
        (clutch--row-identity-cache (make-hash-table :test 'eq))
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
                  (clutch-test--execute-and-present
                   "SELECT id FROM users" 'fake-conn)
                  (should (eq clutch--last-result-buffer
                              (get-buffer result-name))))))))
      (when (buffer-live-p source)
        (kill-buffer source)))))

(ert-deftest clutch-test-high-risk-query-typed-confirmation-contract ()
  "Typed high-risk confirmation should proceed only for the exact token YES."
  (dolist (case '(("NO" cancel)
                  ("YES" accept)))
    (pcase-let ((`(,answer ,expected) case))
      (ert-info ((format "answer: %s" answer))
        (let ((clutch-high-risk-query-confirmation 'typed))
          (cl-letf (((symbol-function 'clutch--high-risk-query-reason)
                     (lambda (_sql) "no WHERE"))
                    ((symbol-function 'read-string)
                     (lambda (&rest _args) answer)))
            (if (eq expected 'accept)
                (should (clutch--confirm-high-risk-query
                         "UPDATE users SET x=1"))
              (should-error (clutch--confirm-high-risk-query
                             "UPDATE users SET x=1")
                            :type 'user-error))))))))

(ert-deftest clutch-test-truncate-requires-typed-confirmation-by-default ()
  "TRUNCATE should require one typed confirmation by default."
  (should (eq (default-value 'clutch-high-risk-query-confirmation) 'typed))
  (let (typed-prompts)
    (cl-letf (((symbol-function 'read-string)
               (lambda (prompt &rest _args)
                 (push prompt typed-prompts)
                 "YES"))
              ((symbol-function 'yes-or-no-p)
               (lambda (&rest _args)
                 (ert-fail "TRUNCATE used a simple confirmation"))))
      (should-not (clutch--confirm-query-execution
                   "TRUNCATE TABLE users"))
      (should (= (length typed-prompts) 1)))))

(ert-deftest clutch-test-high-risk-query-confirmation-is-customizable ()
  "High-risk confirmation should support simple and disabled policies."
  (dolist (policy '(yes-or-no nil))
    (let ((clutch-high-risk-query-confirmation policy)
          (simple-prompts 0))
      (cl-letf (((symbol-function 'read-string)
                 (lambda (&rest _args)
                   (ert-fail "Non-typed policy requested typed confirmation")))
                ((symbol-function 'yes-or-no-p)
                 (lambda (&rest _args)
                   (cl-incf simple-prompts)
                   t)))
        (should-not (clutch--confirm-query-execution
                     "TRUNCATE TABLE users"))
        (should (= simple-prompts (if policy 1 0)))))))

(ert-deftest clutch-test-confirmation-covers-statements-in-with-clause ()
  "A DELETE in or after a WITH clause should ask as a plain DELETE does."
  (dolist (case '(("WITH d AS (DELETE FROM users RETURNING id) SELECT * FROM d"
                   typed)
                  ("WITH d AS (DELETE FROM users WHERE id = 1 RETURNING id) SELECT * FROM d"
                   simple)
                  ("WITH old AS (SELECT 1) DELETE FROM users WHERE id = 1"
                   simple)
                  ("WITH i AS (INSERT INTO users VALUES (1) RETURNING id) SELECT * FROM i"
                   nil)))
    (pcase-let ((`(,sql ,expected) case))
      (ert-info ((format "sql: %s" sql))
        (let ((clutch-high-risk-query-confirmation 'typed)
              prompts)
          (cl-letf (((symbol-function 'read-string)
                     (lambda (&rest _args) (push 'typed prompts) "YES"))
                    ((symbol-function 'yes-or-no-p)
                     (lambda (&rest _args) (push 'simple prompts) t)))
            (clutch--confirm-query-execution sql)
            (should (equal prompts (and expected (list expected))))))))))

(ert-deftest clutch-test-preview-execution-sql-uses-result-pending-batch ()
  "Preview in result mode should show generated SQL for staged result changes."
  (with-temp-buffer
    (let (captured)
      (setq-local clutch-connection 'fake-conn
                  clutch--result-source-table "users"
                  clutch--result-columns '("id" "name" "note")
                  clutch--result-column-defs
                  '((:name "id" :source-column "id")
                    (:name "name" :source-column "name")
                    (:name "note" :source-column "note"))
                  clutch--row-identity
                  (clutch-test--primary-row-identity "users" '("id") '(0))
                  clutch--pending-inserts
                  '((("id" . "3") ("name" . "cat") ("note" . "NULL")))
                  clutch--pending-edits
                  (list (cons (cons (vector 1) 1) "lynx"))
                  clutch--pending-deletes
                  (list (vector 2)))
      (cl-letf (((symbol-function 'derived-mode-p) (lambda (&rest _modes) t))
                ((symbol-function 'clutch--ensure-column-details)
                 (lambda (&rest _)
                   '((:name "id") (:name "name") (:name "note"))))
                ((symbol-function 'clutch-db-escape-identifier)
                 (lambda (_conn name) (format "`%s`" name)))
                ((symbol-function 'clutch-db-escape-literal)
                 (lambda (_conn value) (format "'%s'" value)))
                ((symbol-function 'clutch--preview-sql-buffer)
                 (lambda (sql &optional _product) (setq captured sql))))
        (clutch-preview-execution-sql)
        (should (equal captured
                       (mapconcat #'identity
                                  '("INSERT INTO `users` (`id`, `name`, `note`) VALUES ('3', 'cat', 'NULL');"
                                    "UPDATE `users` SET `name` = 'lynx' WHERE `id` = 1;"
                                    "DELETE FROM `users` WHERE `id` = 2;")
                                  "\n")))))))

(ert-deftest clutch-test-result-effective-query-applies-where-filter ()
  :tags '(:smoke)
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

(ert-deftest clutch-test-execute-page-reapplies-active-local-sort ()
  "Paging should apply an active local sort to each newly loaded page."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn
                clutch--base-query "SELECT id, score FROM complex_result"
                clutch--result-server-pageable t
                clutch--result-server-rewritable nil
                clutch--result-columns '("score" "score")
                clutch--result-column-defs '((:name "score") (:name "score"))
                clutch--sort-column "score"
                clutch--sort-descending nil
                clutch--local-sort-column-index 1
                clutch--order-by nil
                clutch-result-max-rows 3)
    (cl-letf (((symbol-function 'clutch--ensure-connection) #'ignore)
              ((symbol-function 'clutch-db-build-paged-sql)
               (lambda (&rest _args) "SELECT page"))
              ((symbol-function 'clutch--run-db-query)
               (lambda (_conn _sql)
                 (make-clutch-db-result
                  :columns '((:name "id") (:name "score"))
                  :rows '((4 30) (5 10) (6 20)))))
              ((symbol-function 'clutch--refresh-display) #'ignore)
              ((symbol-function 'message) #'ignore))
      (clutch-result--execute-page 1)
      (should (equal (mapcar #'car clutch--result-rows) '(5 6 4)))
      (should (= clutch--local-sort-column-index 1))
      (should (equal clutch--local-sort-original-rows
                     '((4 30) (5 10) (6 20)))))))

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

(ert-deftest clutch-test-result-query-commands-ensure-connection-before-query ()
  "Result query commands should reconnect before querying."
  (dolist (command '(page count))
    (ert-info ((format "command: %s" command))
      (with-temp-buffer
        (let (ensured captured-conn)
          (setq-local clutch-connection 'stale-conn
                      clutch--base-query "SELECT * FROM t"
                      clutch--result-server-pageable t
                      clutch--result-server-rewritable t
                      clutch-result-max-rows 100)
          (cl-letf (((symbol-function 'clutch--ensure-connection)
                     (lambda ()
                       (setq ensured t)
                       (setq-local clutch-connection 'new-conn)))
                    ((symbol-function 'clutch-db-build-paged-sql)
                     (lambda (_conn _sql _page-num _page-size
                              &optional _order-by _page-offset)
                       "SELECT * FROM paged"))
                    ((symbol-function 'clutch-db-build-count-sql)
                     (lambda (_conn _sql) "SELECT COUNT(*)"))
                    ((symbol-function 'clutch-db-query)
                     (lambda (conn _sql)
                       (setq captured-conn conn)
                       (make-clutch-db-result :columns nil :rows '((3)))))
                    ((symbol-function 'clutch--refresh-display) #'ignore)
                    ((symbol-function 'clutch--refresh-footer-line) #'ignore)
                    ((symbol-function 'message) #'ignore))
            (pcase command
              ('page (clutch-result--execute-page 0))
              ('count
               (clutch-result-count-total)
               (should (= clutch--page-total-rows 3))))
            (should ensured)
            (should (eq captured-conn 'new-conn))))))))

(ert-deftest clutch-test-execute-page-remembers-error-details-and-debug-event ()
  "Paging failures should populate `current-buffer' error details and trace.
The page stays, and the message says so."
  (with-temp-buffer
    (let ((conn (make-clutch-db-sqlite-conn :database "/tmp/debug.db"))
          (clutch-debug-mode t)
          (raw-message "Connection refused (host=db.example.com, port=3306)")
          messages)
      (clutch--clear-debug-capture)
      (setq-local clutch-connection conn
                  clutch--base-query "SELECT * FROM t"
                  clutch--result-server-pageable t
                  clutch--result-rows '((1))
                  clutch-result-max-rows 100)
      (cl-letf (((symbol-function 'clutch--ensure-connection) #'ignore)
                ((symbol-function 'clutch--connection-alive-p) (lambda (_conn) t))
                ((symbol-function 'message)
                 (lambda (format-string &rest args)
                   (push (apply #'format format-string args) messages)))
                ((symbol-function 'clutch-db-build-paged-sql)
                 (lambda (_conn _sql _page-num _page-size
                              &optional _order-by _page-offset)
                   "SELECT * FROM t LIMIT 100 OFFSET 0"))
                ((symbol-function 'clutch-db-backend-key)
                 (lambda (_conn) 'pg))
                ((symbol-function 'clutch--run-db-query)
                 (lambda (_conn _sql)
                   (signal 'clutch-db-error (list raw-message)))))
        (let ((display-summary
               (condition-case err
                   (signal 'clutch-db-error (list raw-message))
                 (clutch-db-error
                  (clutch--humanize-db-error (error-message-string err))))))
          (clutch-result--execute-page 0)
          (should (equal clutch--result-rows '((1))))
          (should (string-suffix-p "(result unchanged)" (car messages)))
          (let* ((details clutch--buffer-error-details)
                 (diag (plist-get details :diag))
                 (debug-text (clutch-test--debug-buffer-string)))
            (should details)
            (should (eq (plist-get details :backend) 'pg))
            (should (equal (plist-get details :summary)
                           (clutch--humanize-db-error raw-message)))
            (should (equal (plist-get diag :raw-message) raw-message))
            (should (equal (plist-get (plist-get diag :context) :sql)
                           "SELECT * FROM t"))
            (should (string-match-p "Phase: error" debug-text))
            (should (string-match-p
                     (regexp-quote display-summary) debug-text))))))))

(ert-deftest clutch-test-execute-dml-skips-debug-backend-lookup-when-disabled ()
  "DML execution should not consult debug-only backend state when debug is off."
  (with-temp-buffer
    (let ((clutch-debug-mode nil)
          rendered)
      (cl-letf (((symbol-function 'clutch-db-backend-key)
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
        (should (clutch-test--execute-and-present
                 "UPDATE demo SET enabled = 1 WHERE id = 1" 'fake-conn))
        (should (equal (cadr rendered)
                       "UPDATE demo SET enabled = 1 WHERE id = 1"))
        (should (= (clutch-db-result-affected-rows (car rendered)) 1))))))

(ert-deftest clutch-test-result-sql-commands-use-effective-filtered-query ()
  "Result SQL commands should consume the active filtered query."
  (dolist (command '(rerun preview))
    (ert-info ((format "command: %s" command))
      (with-temp-buffer
        (clutch-result-mode)
        (let (captured)
          (setq-local clutch-connection 'fake-conn
                      clutch--base-query "SELECT * FROM t"
                      clutch--where-filter "id = 1")
          (cl-letf (((symbol-function 'clutch-db-apply-where)
                     (lambda (_conn sql filter)
                       (format "FILTER[%s]{%s}" filter sql)))
                    ((symbol-function 'clutch--execute)
                     (lambda (sql &optional context)
                       (setq captured (list sql context))))
                    ((symbol-function 'clutch--preview-sql-buffer)
                     (lambda (sql &optional _product)
                       (setq captured sql))))
            (pcase command
              ('rerun
               (clutch-result-rerun)
               (should (equal (car captured)
                              "FILTER[id = 1]{SELECT * FROM t}"))
               ;; The filter goes with it, so the new result keeps it.
               (should (equal (plist-get (nth 1 captured) :where-filter)
                              "id = 1")))
              ('preview
               (clutch-preview-execution-sql)
               (should (equal captured
                              "FILTER[id = 1]{SELECT * FROM t}"))))))))))

(ert-deftest clutch-test-preview-execution-sql-prefers-semicolon-statement-bounds-in-sql-buffer ()
  "Preview should mirror DWIM statement bounds for semicolon-delimited SQL buffers."
  (with-temp-buffer
    (insert "INSERT INTO demo(note) VALUES (E'first line\n\nthird line');\n\nSELECT 2")
    (goto-char (point-min))
    (search-forward "third")
    (let (captured)
      (cl-letf (((symbol-function 'clutch--preview-sql-buffer)
                 (lambda (sql &optional _product) (setq captured sql))))
        (clutch-preview-execution-sql)
        (should (equal captured
                       "INSERT INTO demo(note) VALUES (E'first line\n\nthird line')"))))))

(ert-deftest clutch-test-preview-leaves-the-result-buffer-selected ()
  "\\`C-c C-c' after \\`C-c C-p' should submit from the result buffer.
The preview is in `sql-mode', where \\`C-c C-c' sends to an SQL process."
  (let ((result (generate-new-buffer " *clutch-preview-focus*")))
    (unwind-protect
        (save-window-excursion
          (delete-other-windows)
          (split-window)
          (switch-to-buffer result)
          (clutch-result-mode)
          (cl-letf (((symbol-function 'clutch-result--preview-execution-sql)
                     (lambda () "UPDATE t SET a = 1 WHERE id = 1;")))
            (call-interactively (key-binding (kbd "C-c C-p"))))
          (should (eq (window-buffer (selected-window)) result))
          (should (eq (key-binding (kbd "C-c C-c")) #'clutch-result-submit))
          (should (get-buffer-window "*clutch-preview*")))
      (kill-buffer result)
      (when-let* ((preview (get-buffer "*clutch-preview*")))
        (kill-buffer preview)))))

(ert-deftest clutch-test-preview-sql-buffer-uses-local-connection-product ()
  "SQL previews should use their source dialect without changing the default."
  (let ((default-product (default-value 'sql-product))
        preview-buffer)
    (unwind-protect
        (cl-letf (((symbol-function 'display-buffer)
                   (lambda (buf &rest _args)
                     (setq preview-buffer buf)
                     nil)))
          (clutch--preview-sql-buffer
           "SELECT NVL(name, 0) FROM DUAL" 'oracle)
          (with-current-buffer preview-buffer
            (font-lock-ensure)
            (should (local-variable-p 'sql-product))
            (should (eq sql-product 'oracle))
            (goto-char (point-min))
            (search-forward "NVL")
            (should (get-text-property (match-beginning 0) 'face)))
          (should (eq (default-value 'sql-product) default-product)))
      (when (buffer-live-p preview-buffer)
        (kill-buffer preview-buffer)))))

(ert-deftest clutch-test-statement-breaks-respect-mysql-backslash-escapes ()
  "A MySQL escaped quote must not end the literal and split the statement.
Splitting there would send the fragment before the semicolon as a whole
statement."
  (let* ((sql "UPDATE t SET note = 'it\\'s here; keep' WHERE id = 1; SELECT 2;")
         (mysql (clutch-db-sql-dialect 'mysql))
         (inside (string-search "; keep" sql)))
    ;; Only the two real terminators, both outside the literal.
    (should (equal (clutch-db-sql-statement-breaks sql mysql)
                   (list (string-search ";" sql (string-search "id = 1" sql))
                         (1- (length sql)))))
    ;; Without the dialect the escaped quote is read as the literal's end, so
    ;; the semicolon inside the value is taken for a statement terminator.
    (should (member inside (clutch-db-sql-statement-breaks sql)))
    (should-not (member inside (clutch-db-sql-statement-breaks sql mysql)))
    ;; A trailing backslash keeps its standard meaning where the dialect has
    ;; no backslash escape, so the literal still ends at the closing quote.
    (should (= (length (clutch-db-sql-statement-breaks
                        "SELECT 'a\\'; SELECT 2;"))
               2))
    (should (equal (clutch-db-sql-mask-literal-or-comment
                    "SELECT 'a\\'b' FROM t" mysql)
                   "SELECT '    ' FROM t"))))

(ert-deftest clutch-test-sql-dialect-rules ()
  "Dialect lookup should only claim rules a product actually has."
  (should (equal (clutch-db-sql-dialect 'postgres) '(:dollar-quotes t)))
  (should (equal (clutch-db-sql-dialect 'mysql) '(:backslash-escapes t)))
  (dolist (product '(sqlite oracle ms db2 nil))
    (should-not (clutch-db-sql-dialect product))))

(ert-deftest clutch-test-statement-breaks-ignore-postgresql-dollar-quotes ()
  "Dollar-quoted function bodies should remain one executable statement."
  (dolist (case '(("$$" . "PERFORM 1; PERFORM 2;")
                  ("$body$" . "SELECT ';'; RETURN;")))
    (let* ((pg (clutch-db-sql-dialect 'postgres))
           (delimiter (car case))
           (body (cdr case))
           (sql (format (concat "CREATE FUNCTION f() RETURNS void AS %s%s%s "
                                "LANGUAGE plpgsql; SELECT 2;")
                        delimiter body delimiter))
           (body-open (string-search delimiter sql))
           (body-close (+ (string-search delimiter sql
                                          (+ body-open (length delimiter)))
                          (length delimiter)))
           (breaks (clutch-db-sql-statement-breaks sql pg)))
      (ert-info ((format "delimiter: %s" delimiter))
        (should (= (length breaks) 2))
        (should (cl-every (lambda (offset) (>= offset body-close)) breaks)))))
  (should (= (length (clutch-db-sql-statement-breaks
                      "SELECT $1; SELECT 2;" (clutch-db-sql-dialect 'postgres)))
             2))
  (should (= (length (clutch-db-sql-statement-breaks
                      "SELECT $tag$; SELECT 2;"))
             2))
  (should (= (length (clutch-db-sql-statement-breaks
                      "SELECT foo$tag$; SELECT 2;"
                      (clutch-db-sql-dialect 'postgres)))
             2))
  (string-match "needle" "needle")
  (let ((saved-match-data (match-data)))
    (clutch-db-sql-statement-breaks
     (concat "SELECT " (mapconcat #'identity
                                  (make-list 4000 "$1") ",") ";")
     (clutch-db-sql-dialect 'postgres))
    (should (equal (match-data) saved-match-data))))

(ert-deftest clutch-test-postgresql-statement-bounds-enable-dollar-quotes ()
  "Query bounds should enable dollar quotes only for PostgreSQL products."
  (let ((sql (concat "CREATE FUNCTION f() RETURNS void AS $$"
                     "PERFORM 1; PERFORM 2;"
                     "$$ LANGUAGE plpgsql; SELECT 2;")))
    (with-temp-buffer
      (insert sql)
      (search-backward "PERFORM 1")
      (let ((clutch--conn-sql-product 'postgres))
        (pcase-let ((`(,beg . ,end) (clutch--statement-bounds-at-point)))
          (should (equal (string-trim
                          (buffer-substring-no-properties beg end))
                         (substring sql 0 (string-search "; SELECT" sql)))))))))

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
              (conn (make-clutch-db-sqlite-conn :database "/tmp/debug.db"))
              (raw-message "Connection refused (host=db.example.com, port=3306)")
              displayed)
          (clutch--clear-debug-capture)
          (setq-local clutch-connection conn)
          (cl-letf (((symbol-function 'clutch-db-backend-key)
                     (lambda (_conn) 'mysql))
                    ((symbol-function 'clutch-result--display-error)
                     (lambda (_conn sql summary message &optional _elapsed hint)
                       (setq displayed (list sql summary message hint))
                       (current-buffer)))
                    ((symbol-function 'clutch--run-db-query)
                     (lambda (_conn sql)
                       (if (equal sql broken-sql)
                           (signal 'clutch-db-error (list raw-message))
                         (make-clutch-db-result :affected-rows 1)))))
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
              (should (eq clutch-connection conn))
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
                     (debug-text (clutch-test--debug-buffer-string)))
                (should details)
                (should (eq (plist-get details :backend) 'mysql))
                (should (equal (plist-get diag :raw-message) raw-message))
                (should (equal (plist-get (plist-get diag :context) :sql)
                               broken-sql))
                (should (string-match-p "Phase: error" debug-text))
                (should (string-match-p
                         (regexp-quote display-summary) debug-text))))))))))

(ert-deftest clutch-test-execution-error-renders-result-without-message ()
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
        (clutch--present-statement-outcome
         "SELECT * FROM missing_users" 'fake-conn
         (list :error err :elapsed 0.012 :source-buffer (current-buffer))
         (cons (point-min) (point-max)))
        (should displayed)
        (should (equal (car displayed) "SELECT * FROM missing_users"))
        (should-not messages)
        (should (overlayp clutch--executed-sql-overlay))
        (should (string-match-p
                 "Last failed SQL"
                 (overlay-get clutch--executed-sql-overlay 'help-echo)))
        (let* ((before (overlay-get clutch--executed-sql-overlay 'before-string))
               (display (get-text-property 0 'display before)))
          (if (display-graphic-p)
              (should (equal display
                             '(left-fringe clutch-executed-sql-dot
                                           clutch-failed-sql-marker-face)))
            (should (equal (car display) '(margin left-margin)))
            (should (equal (cadr display) "●"))
            (should (eq (get-text-property 0 'face (cadr display))
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
      (if (display-graphic-p)
          (should (equal display
                         '(left-fringe clutch-executed-sql-dot
                                       clutch-executed-sql-marker-face)))
         (should (equal (car display) '(margin left-margin)))
         (should (equal (cadr display) "●"))
         (should (eq (get-text-property 0 'face (cadr display))
                    'clutch-executed-sql-marker-face))))
    (should-not (overlay-get clutch--executed-sql-overlay 'modification-hooks))))

(ert-deftest clutch-test-execute-quit-distinguishes-confirmation-from-query ()
  "Only a quit during the database call should retire the connection."
  (with-temp-buffer
    (let ((buf (current-buffer))
          (disconnected nil)
          confirmation-quit
          phase
          refresh-started
          (clutch--tx-state-cache (make-hash-table :test 'eq))
          (clutch-connection 'fake-conn)
          (clutch--execution-start-time nil))
      (puthash clutch-connection 'dirty clutch--tx-state-cache)
      (cl-letf (((symbol-function 'clutch--ensure-connection) (lambda () t))
                ((symbol-function 'clutch-result--check-pending-changes) #'ignore)
                ((symbol-function 'clutch--execution-refresh-start)
                 (lambda () (setq refresh-started t)))
                ((symbol-function 'clutch--update-mode-line)
                 (lambda (&optional _execution-only) nil))
                ((symbol-function 'clutch--confirm-query-execution)
                 (lambda (_sql)
                   (when (eq phase 'confirm) (signal 'quit nil))))
                ((symbol-function 'clutch-db-result-query-p)
                 (lambda (&rest _args) t))
                ((symbol-function 'clutch--prepare-row-identity-query)
                 (lambda (&rest _args)
                   (should refresh-started)
                   (signal 'quit nil)))
                ((symbol-function 'clutch--connection-alive-p) (lambda (_conn) t))
                ((symbol-function 'clutch-db-interrupt-query) (lambda (_conn) nil))
                ((symbol-function 'clutch-db-disconnect)
                 (lambda (_conn) (setq disconnected t))))
        (setq phase 'confirm)
        (condition-case nil
            (clutch--execute "SELECT 1")
          (quit (setq confirmation-quit t)))
        (should confirmation-quit)
        (should-not disconnected)
        (should (eq clutch-connection 'fake-conn))
        (setq phase 'query)
        (let ((error (should-error
                      (clutch--execute "SELECT 1")
                      :type 'user-error)))
          (should (equal (cadr error)
                         clutch--transaction-outcome-unknown-message)))
        (should disconnected)
        (with-current-buffer buf
          (should (eq clutch-connection 'fake-conn)))
        ;; Retirement keeps the dirty flag as lost-transaction evidence;
        ;; the next transaction command consumes it and refuses to run.
        (should (gethash 'fake-conn clutch--tx-state-cache))
        (should-not clutch--execution-start-time)))))

(ert-deftest clutch-test-execute-quit-prefers-backend-interrupt-over-disconnect ()
  "Quit should keep the session when a backend interrupt succeeds."
  (with-temp-buffer
    (let* ((buf (current-buffer))
           (conn 'fake-conn)
           (interrupted nil)
           (disconnected nil)
           (clutch--tx-state-cache (make-hash-table :test 'eq))
           (clutch-connection conn)
           (clutch--execution-start-time nil))
      (cl-letf (((symbol-function 'clutch--ensure-connection) (lambda () t))
                ((symbol-function 'clutch-result--check-pending-changes) #'ignore)
                ((symbol-function 'clutch--update-mode-line)
                 (lambda (&optional _execution-only) nil))
                ((symbol-function 'clutch--execute-statement)
                 (lambda (_sql connection &rest _args)
                   (clutch--handle-query-quit connection)))
                ((symbol-function 'clutch--connection-alive-p) (lambda (_conn) t))
                ((symbol-function 'clutch-db-interrupt-query)
                 (lambda (_conn)
                   (setq interrupted t)
                   t))
                ((symbol-function 'clutch-db-disconnect)
                 (lambda (_conn) (setq disconnected t))))
        (should-error (clutch--execute "SELECT pg_sleep(10)")
                      :type 'user-error)
        (should interrupted)
        (should-not disconnected)
        (with-current-buffer buf
          (should (eq clutch-connection conn)))
        (should-not clutch--execution-start-time)))))

(ert-deftest clutch-test-execute-db-error-preserves-dead-reconnect-anchor ()
  "Query errors should retain a dead connection for the next reconnect."
  (with-temp-buffer
    (let* ((conn 'fake-conn)
           (clutch-connection conn)
           (clutch--tx-state-cache (make-hash-table :test 'eq))
           (clutch--execution-start-time nil)
           (displayed-error nil)
           (error-context nil)
           (preserved nil)
           (details-cleared nil)
           (executions 0)
           (mode-line-updates 0))
      (puthash conn 'dirty clutch--tx-state-cache)
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
                ((symbol-function 'clutch-db-sql-has-top-level-row-limit-p)
                 (lambda (_sql) t))
                ((symbol-function 'clutch--connection-alive-p)
                 (lambda (_conn) nil))
                ((symbol-function 'clutch--run-db-query)
                 (lambda (&rest _args)
                   (setq executions (1+ executions))
                   (signal 'clutch-db-error '("query timed out"))))
                ((symbol-function 'clutch--show-execution-error)
                 (lambda (_source _conn _sql err &optional _elapsed context _region)
                   (setq displayed-error (error-message-string err)
                         error-context context)))
                ((symbol-function 'clutch--preserve-dead-connection-for-reconnect)
                 (lambda (connection)
                   (setq preserved connection)))
                ((symbol-function 'clutch-db-clear-error-details)
                 (lambda (connection)
                   (setq details-cleared connection)))
                ((symbol-function 'clutch--update-mode-line)
                 (lambda (&optional _execution-only)
                   (setq mode-line-updates (1+ mode-line-updates)))))
        (clutch--execute "SELECT SLEEP(60)")
        (should (= executions 1))
        (should (string-match-p "query timed out" displayed-error))
        (should (eq (plist-get error-context :transaction-outcome) 'unknown))
        (should (eq clutch-connection conn))
        (should (eq preserved conn))
        (should (eq details-cleared conn))
        (should-not clutch--execution-start-time)
        (should (> mode-line-updates 0))))))

(ert-deftest clutch-test-replacement-connection-keeps-the-commit-mode ()
  "A connection that replaces another should keep its commit mode.
A console in Manual mode came back in Auto mode after the automatic
reconnect, so each statement after it was committed on its own.  A new
connection that cannot take the mode is discarded, and one that has no
Manual mode is left as it is."
  (clutch-test--with-isolated-metadata-caches
    (let ((old-conn (list 'old-connection))
          (new-conn (list 'new-connection))
          (clutch--tx-state-cache (make-hash-table :test 'eq))
          modes set discarded)
      (cl-letf (((symbol-function 'clutch--build-conn) (lambda (_params) new-conn))
                ((symbol-function 'clutch-db-manual-commit-supported-p)
                 (lambda (_conn) t))
                ((symbol-function 'clutch-db-manual-commit-p)
                 (lambda (conn) (alist-get conn modes)))
                ((symbol-function 'clutch-db-set-auto-commit)
                 (lambda (conn auto-commit)
                   (push (list conn auto-commit) set)
                   (setf (alist-get conn modes) (not auto-commit))))
                ((symbol-function 'clutch--discard-unbound-connection)
                 (lambda (conn) (push conn discarded))))
        (cl-flet ((replace (old-manual new-manual)
                    (setq modes (list (cons old-conn old-manual)
                                      (cons new-conn new-manual))
                          set nil)
                    (should (eq (clutch--build-replacement-conn old-conn nil)
                                new-conn))
                    set))
          (should (equal (replace t nil) (list (list new-conn nil))))
          ;; Oracle starts in manual mode, which a session may have left.
          (should (equal (replace nil t) (list (list new-conn t))))
          (should-not (replace t t))
          (should-not (replace nil nil))
          (cl-letf (((symbol-function 'clutch-db-manual-commit-supported-p)
                     (lambda (conn) (eq conn old-conn))))
            (should-not (replace t nil)))
          (cl-letf (((symbol-function 'clutch-db-manual-commit-supported-p)
                     (lambda (conn) (eq conn new-conn))))
            (should (equal (replace t t) (list (list new-conn t))))))
        (dolist (failure '((clutch-db-error "Lost connection") (quit)))
          (setq modes (list (cons old-conn t) (cons new-conn nil))
                discarded nil)
          (cl-letf (((symbol-function 'clutch-db-set-auto-commit)
                     (lambda (_conn _auto-commit)
                       (signal (car failure) (cdr failure)))))
            (should (eq (car (condition-case err
                                 (clutch--build-replacement-conn old-conn nil)
                               ((error quit) err)))
                        (car failure))))
          (should (equal discarded (list new-conn))))
        ;; A schema switch that reconnects goes through it too.
        (setq modes (list (cons old-conn t) (cons new-conn nil))
              set nil)
        (cl-letf (((symbol-function 'clutch--connection-alive-p) #'ignore)
                  ((symbol-function 'clutch--require-live-connection) #'ignore)
                  ((symbol-function 'clutch--finalize-rebound-connection) #'ignore))
          (clutch--replace-connection old-conn nil 'postgres))
        (should (equal set (list (list new-conn nil))))))))

(ert-deftest clutch-test-dead-query-reconnects-on-next-command-without-replay ()
  "A dead query should preserve its session anchor until the next command."
  (clutch-test--with-isolated-metadata-caches
    (let* ((old-conn (list 'old-connection))
           (new-conn (list 'new-connection))
           (params '(:backend oracle :database "ORCL"))
           (source (generate-new-buffer " *clutch-reconnect-source*"))
           (attached (generate-new-buffer " *clutch-reconnect-attached*"))
           (clutch--tx-state-cache (make-hash-table :test 'eq))
           (clutch--problem-records-by-conn (make-hash-table :test 'eq))
           (clutch--schema-cache-updated-hook
            '(clutch--handle-schema-cache-updated))
           (clutch--metadata-state-changed-hook
            '(clutch--refresh-schema-status-ui))
           (old-live t)
           allow-revert
           (builds 0)
           (reverts 0)
           executions)
      (unwind-protect
          (progn
            (dolist (buffer (list source attached))
              (with-current-buffer buffer
                (setq-local clutch-connection old-conn
                            clutch--connection-params params
                            clutch--conn-sql-product 'oracle)))
            (with-current-buffer source
              (setq-local clutch--query-buffer-local-p t))
            (with-current-buffer attached
              (setq-local revert-buffer-function
                          (lambda (&rest _args)
                            (unless allow-revert
                              (ert-fail "Dead-session cleanup must not revert"))
                            (cl-incf reverts))))
            (cl-letf (((symbol-function 'clutch--connection-alive-p)
                       (lambda (connection)
                         (if (eq connection old-conn)
                             old-live
                           (eq connection new-conn))))
                      ((symbol-function 'clutch--build-conn)
                       (lambda (reconnect-params)
                         (should (equal reconnect-params params))
                         (cl-incf builds)
                         new-conn))
                      ((symbol-function 'clutch--execute-statement)
                       (lambda (sql connection _present-result-p _region k
                                    &rest _args)
                         (push (list sql connection) executions)
                         (funcall
                          k
                          (if (eq connection old-conn)
                              (progn
                                (setq old-live nil)
                                (list :error '(clutch-db-error "socket lost")
                                      :source-buffer source))
                            (list :result (make-clutch-db-result :affected-rows 1)
                                  :result-query-p nil
                                  :source-buffer source)))))
                      ((symbol-function 'clutch--show-execution-error)
                       (lambda (&rest _args) "socket lost"))
                      ((symbol-function 'clutch-result--display) #'ignore)
                      ((symbol-function 'clutch-db-clear-error-details) #'ignore)
                      ((symbol-function 'clutch--prime-schema-cache) #'ignore)
                      ((symbol-function 'clutch--refresh-transaction-ui) #'ignore)
                      ((symbol-function 'clutch--refresh-connection-render-state)
                       #'ignore)
                      ((symbol-function 'clutch--execution-refresh-start) #'ignore)
                      ((symbol-function 'clutch--update-mode-line) #'ignore)
                      ((symbol-function 'redisplay) #'ignore)
                      ((symbol-function 'clutch--connection-key)
                       (lambda (_connection) "oracle@test"))
                      ((symbol-function 'message) #'ignore))
              (with-current-buffer source
                (clutch--execute "SELECT once")
                (should (= (length executions) 1))
                (should (= builds 0))
                (should (= reverts 0))
                (should (eq clutch-connection old-conn))
                (setq allow-revert t)
                (clutch--execute "SELECT next")))
            (should (= builds 1))
            (should (= reverts 1))
            (should (equal (nreverse executions)
                           `(("SELECT once" ,old-conn)
                             ("SELECT next" ,new-conn))))
            (dolist (buffer (list source attached))
              (should (eq (buffer-local-value 'clutch-connection buffer)
                          new-conn))))
        (dolist (buffer (list source attached))
          (when (buffer-live-p buffer)
            (kill-buffer buffer)))))))

(ert-deftest clutch-test-execute-retries-only-safe-clean-preflight-failures ()
  "Retry once only when JDBC proves execution did not start and tx is clean."
  (dolist (case '((auto nil nil 2 1 new-conn nil)
                  (manual-clean t nil 2 1 new-conn nil)
                  (manual-dirty t t 1 0 old-conn clutch-db-execution-not-started)
                  (ambiguous-first-failure nil nil 1 0 old-conn clutch-db-error)
                  (second-failure nil nil 2 1 new-conn clutch-db-error)))
    (pcase-let ((`(,label ,manual ,dirty ,expected-runs ,expected-reconnects
                         ,expected-connection ,expected-error)
                 case))
      (with-temp-buffer
        (let ((clutch-connection 'old-conn)
              (clutch--tx-state-cache (make-hash-table :test 'eq))
              (old-live t)
              (runs 0)
              (confirmations 0)
              (reconnects 0)
              (clutch-db--foreground-connections
               (make-hash-table :test 'eq)))
          (when dirty
            (puthash 'old-conn 'dirty clutch--tx-state-cache))
          (cl-letf (((symbol-function 'clutch--confirm-query-execution)
                     (lambda (_sql) (cl-incf confirmations)))
                    ((symbol-function 'clutch-db-result-query-p)
                     (lambda (&rest _args) nil))
                    ((symbol-function 'clutch-db-manual-commit-p)
                     (lambda (_conn) manual))
                    ((symbol-function 'clutch--connection-alive-p)
                     (lambda (conn)
                       (if (eq conn 'old-conn) old-live t)))
                    ((symbol-function 'clutch--run-db-query)
                     (lambda (conn _sql)
                       (should (clutch-db--foreground-busy-p conn))
                       (cl-incf runs)
                       (cond
                        ((eq conn 'old-conn)
                         (setq old-live nil)
                         (signal (if (eq label 'ambiguous-first-failure)
                                     'clutch-db-error
                                   'clutch-db-execution-not-started)
                                 '("idle validation failed")))
                        ((eq label 'second-failure)
                         (signal 'clutch-db-error '("second attempt failed")))
                        (t
                         (make-clutch-db-result :affected-rows 1)))))
                    ((symbol-function 'clutch--try-reconnect)
                     (lambda ()
                       (cl-incf reconnects)
                       (setq clutch-connection 'new-conn)
                       t)))
            (let ((outcome
                   (clutch-test--await-outcome
                    (lambda (k)
                      (clutch--execute-statement
                       "SELECT side_effect_free" 'old-conn nil nil k)))))
              (ert-info ((format "case: %s" label))
                (should (= runs expected-runs))
                ;; Callers confirm once; neither attempt asks again.
                (should (= confirmations 0))
                (should (= reconnects expected-reconnects))
                (should (eq (plist-get outcome :connection)
                            expected-connection))
                (should (eq (car-safe (plist-get outcome :error))
                            expected-error))))))))))

(ert-deftest clutch-test-batch-continues-on-the-reconnected-connection ()
  "Statements after an idle reconnect in a batch should use the new connection."
  (with-temp-buffer
    (let ((clutch-connection 'old-conn)
          (clutch--tx-state-cache (make-hash-table :test 'eq))
          (clutch-db--foreground-connections (make-hash-table :test 'eq))
          (old-live t)
          executions)
      (cl-letf (((symbol-function 'clutch--confirm-query-execution) #'ignore)
                ((symbol-function 'clutch-result--check-pending-changes) #'ignore)
                ((symbol-function 'clutch-db-result-query-p) #'ignore)
                ((symbol-function 'clutch-db-manual-commit-p) #'ignore)
                ((symbol-function 'clutch--forget-row-identities) #'ignore)
                ((symbol-function 'clutch--note-schema-affecting-query) #'ignore)
                ((symbol-function 'clutch--execution-refresh-start) #'ignore)
                ((symbol-function 'clutch--update-mode-line) #'ignore)
                ((symbol-function 'message) #'ignore)
                ((symbol-function 'clutch--connection-key)
                 (lambda (conn) (symbol-name conn)))
                ((symbol-function 'clutch--connection-alive-p)
                 (lambda (conn) (if (eq conn 'old-conn) old-live t)))
                ((symbol-function 'clutch--run-db-query)
                 (lambda (conn sql &rest _args)
                   (push (list sql conn) executions)
                   (if (eq conn 'old-conn)
                       (progn
                         (setq old-live nil)
                         (signal 'clutch-db-execution-not-started
                                 '("idle validation failed")))
                     (make-clutch-db-result :affected-rows 1))))
                ((symbol-function 'clutch--try-reconnect)
                 (lambda ()
                   (setq clutch-connection 'new-conn)
                   t)))
        (clutch--execute-statements '("UPDATE a SET n = 1" "UPDATE b SET n = 2"))
        (should (equal (nreverse executions)
                       '(("UPDATE a SET n = 1" old-conn)
                         ("UPDATE a SET n = 1" new-conn)
                         ("UPDATE b SET n = 2" new-conn))))))))

(ert-deftest clutch-test-batch-stops-when-its-buffer-moves-during-a-synchronous-statement ()
  "A batch should stop when its buffer moves while a statement runs synchronously.
A backend that waits on the network synchronously runs timers, and one of
them can move the buffer to another connection before the reply arrives."
  (with-temp-buffer
    (let ((clutch-connection 'conn-a)
          (clutch--tx-state-cache (make-hash-table :test 'eq))
          (clutch-db--foreground-connections (make-hash-table :test 'eq))
          (source (current-buffer))
          executions messages)
      (cl-letf (((symbol-function 'clutch--confirm-query-execution) #'ignore)
                ((symbol-function 'clutch-result--check-pending-changes) #'ignore)
                ((symbol-function 'clutch-db-result-query-p) #'ignore)
                ((symbol-function 'clutch-db-manual-commit-p) #'ignore)
                ((symbol-function 'clutch--forget-row-identities) #'ignore)
                ((symbol-function 'clutch--note-schema-affecting-query) #'ignore)
                ((symbol-function 'clutch--execution-refresh-start) #'ignore)
                ((symbol-function 'clutch--update-mode-line) #'ignore)
                ((symbol-function 'clutch--connection-alive-p) (lambda (_conn) t))
                ((symbol-function 'clutch--connection-key)
                 (lambda (conn) (symbol-name conn)))
                ((symbol-function 'message)
                 (lambda (format-string &rest args)
                   (push (apply #'format format-string args) messages)))
                ((symbol-function 'clutch--run-db-query)
                 (lambda (conn sql &rest _args)
                   (push (list sql conn) executions)
                   ;; A timer that runs while the statement waits moves the buffer.
                   (with-current-buffer source
                     (setq clutch-connection 'conn-b))
                   (make-clutch-db-result :affected-rows 1))))
        (clutch--execute-statements '("UPDATE a SET n = 1" "UPDATE b SET n = 2"))
        (should (equal executions '(("UPDATE a SET n = 1" conn-a))))
        (should (member "1 statement executed, then stopped: the connection changed"
                        messages))
        (should-not (clutch-db--foreground-busy-p 'conn-a))))))

(ert-deftest clutch-test-batch-stops-when-its-buffer-switches-connection ()
  "A batch should stop rather than follow its buffer to another connection."
  (with-temp-buffer
    (setq-local clutch-connection 'conn-a)
    (clutch-test--with-async-statements finishes
      (let (sent messages)
        (cl-letf (((symbol-function 'clutch-db-query-async)
                   (lambda (conn sql callback)
                     (push (cons conn sql) sent)
                     (push (cons sql callback) finishes)
                     t))
                  ((symbol-function 'message)
                   (lambda (format-string &rest args)
                     (push (apply #'format format-string args) messages))))
          (clutch--execute-statements '("UPDATE t SET n = 1" "UPDATE t SET n = 2"))
          (funcall (cdar finishes) (make-clutch-db-result :affected-rows 1) nil)
          (setq-local clutch-connection 'conn-b)
          (ert-run-idle-timers)
          (should (equal sent '((conn-a . "UPDATE t SET n = 1"))))
          (should (member "1 statement executed, then stopped: the connection changed"
                          messages))
          (should-not (clutch-db--foreground-busy-p 'conn-a)))))))

(ert-deftest clutch-test-batch-reports-a-statement-that-failed-after-its-buffer-moved ()
  "A batch statement that fails after its buffer moved should only be reported.
The disconnect that moved the buffer closed the connection, so the statement
in flight failed with an unknown outcome; drawing that put an error page in
a result buffer of another connection, or of none."
  (with-temp-buffer
    (setq-local clutch-connection 'conn-a)
    (clutch-test--with-async-statements finishes
      (let ((a-alive t) shown messages)
        (cl-letf (((symbol-function 'clutch--connection-key)
                   (lambda (conn) (if conn (symbol-name conn) "none")))
                  ((symbol-function 'clutch--connection-alive-p)
                   (lambda (conn) (or a-alive (not (eq conn 'conn-a)))))
                  ((symbol-function 'clutch--show-execution-error)
                   (lambda (&rest _) (setq shown t) "failed"))
                  ((symbol-function 'clutch--retire-query-connection) #'ignore)
                  ((symbol-function 'message)
                   (lambda (format-string &rest args)
                     (push (apply #'format format-string args) messages))))
          (clutch--execute-statements '("UPDATE t SET n = 1" "UPDATE t SET n = 2"))
          (funcall (cdar finishes) (make-clutch-db-result :affected-rows 1) nil)
          (ert-run-idle-timers)
          ;; Statement 2 runs; a disconnect then clears the buffer's connection.
          (setq a-alive nil)
          (setq-local clutch-connection nil)
          (funcall (cdar finishes) nil
                   '(clutch-db-error
                     "Disconnected while the statement ran; its outcome is unknown"))
          (ert-run-idle-timers)
          (should-not shown)
          (should (cl-some
                   (lambda (text)
                     (string-match-p
                      "\\`1 statement executed, then stopped: the connection changed; statement 2: .*outcome is unknown"
                      text))
                   messages))
          (should-not (clutch-db--foreground-busy-p 'conn-a)))))))

(ert-deftest clutch-test-idle-retry-recomputes-row-identity-on-new-connection ()
  "A physical reconnect should not reuse the old connection's identity plan."
  (with-temp-buffer
    (let ((clutch-connection 'old-conn)
          (old-live t)
          executions prepared-connections)
      (cl-letf (((symbol-function 'clutch--confirm-query-execution) #'ignore)
                ((symbol-function 'clutch-db-result-query-p)
                 (lambda (&rest _args) t))
                ((symbol-function 'clutch-db-query-result-context)
                 (lambda (&rest _args) nil))
                ((symbol-function 'clutch--prepare-row-identity-query)
                 (lambda (connection _sql)
                   (push connection prepared-connections)
                   '(:sql "fresh-plan")))
                ((symbol-function 'clutch--connection-alive-p)
                 (lambda (connection)
                   (if (eq connection 'old-conn) old-live t)))
                ((symbol-function 'clutch--run-db-query)
                 (lambda (connection sql)
                   (push (list connection sql) executions)
                   (if (eq connection 'old-conn)
                       (progn
                         (setq old-live nil)
                         (signal 'clutch-db-execution-not-started
                                 '("idle validation failed")))
                     (make-clutch-db-result :columns ["id"] :rows '((1))))))
                ((symbol-function 'clutch--try-reconnect)
                 (lambda ()
                   (setq clutch-connection 'new-conn)
                   t)))
        (let ((outcome
               (clutch-test--await-outcome
                (lambda (k)
                  (clutch--execute-statement
                   "SELECT * FROM items" 'old-conn t nil k
                   '(:row-identity-prep (:sql "stale-plan")
                     :server-pageable nil))))))
          (should (eq (plist-get outcome :connection) 'new-conn))
          (should (equal (nreverse executions)
                         '((old-conn "stale-plan")
                           (new-conn "fresh-plan"))))
          (should (equal prepared-connections '(new-conn))))))))

(defmacro clutch-test--with-async-statements (finishes-var &rest body)
  "Run BODY with statements finishing only when callbacks in FINISHES-VAR run.
Each started statement pushes (SQL . CALLBACK) onto FINISHES-VAR."
  (declare (indent 1) (debug (symbolp body)))
  `(let ((clutch-db--foreground-connections (make-hash-table :test 'eq))
         (clutch--running-queries (make-hash-table :test 'eq))
         (,finishes-var nil))
     (cl-letf (((symbol-function 'clutch--connection-alive-p) (lambda (_conn) t))
               ((symbol-function 'clutch-result--check-pending-changes) #'ignore)
               ((symbol-function 'clutch--update-mode-line) #'ignore)
               ((symbol-function 'clutch--execution-refresh-start) #'ignore)
               ((symbol-function 'clutch--confirm-query-execution) #'ignore)
               ((symbol-function 'clutch-db-result-query-p) #'ignore)
               ((symbol-function 'clutch-db-manual-commit-p) #'ignore)
               ((symbol-function 'clutch--tx-uncertain-p) #'ignore)
               ((symbol-function 'clutch--record-tx-state-after-query) #'ignore)
               ((symbol-function 'clutch--note-schema-affecting-query) #'ignore)
               ((symbol-function 'clutch-db-query-async)
                (lambda (_conn sql callback)
                  (push (cons sql callback) ,finishes-var)
                  t)))
       ,@body)))

(ert-deftest clutch-test-indirect-execute-runs-in-a-buffer-holding-its-connection ()
  "SQL from an indirect edit should run in a buffer that holds its connection.
Closing the edit shows a buffer of another kind, such as source code, which
holds no connection for the statement to belong to."
  (let ((console (generate-new-buffer " *clutch-test-console*"))
        (code (generate-new-buffer " *clutch-test-code*"))
        (indirect (generate-new-buffer " *clutch-test-indirect*"))
        ran-in)
    (unwind-protect
        (progn
          (with-current-buffer console
            (setq-local clutch-connection 'indirect-conn))
          (with-current-buffer indirect
            (setq-local clutch-connection 'indirect-conn)
            (insert "SELECT 1"))
          (switch-to-buffer code)
          (switch-to-buffer indirect)
          (cl-letf (((symbol-function 'clutch--connection-alive-p)
                     (lambda (_conn) t))
                    ((symbol-function 'clutch--execute)
                     (lambda (sql &rest _)
                       (setq ran-in (list (current-buffer) sql)))))
            (with-current-buffer indirect
              (clutch-indirect-execute)))
          (should-not (buffer-live-p indirect))
          (should (equal ran-in (list console "SELECT 1"))))
      (dolist (buffer (list console code indirect))
        (when (buffer-live-p buffer)
          (kill-buffer buffer))))))

(ert-deftest clutch-test-indirect-execute-runs-in-the-edit-holding-its-connection-alone ()
  "SQL from an indirect edit that alone holds its connection should run there.
Connecting in the edit leaves no other buffer to run the SQL in.  The edit is
buried instead of killed, since its statement's reply needs it."
  (let ((code (generate-new-buffer " *clutch-test-code*"))
        (indirect (generate-new-buffer " *clutch-test-indirect*"))
        ran-in)
    (unwind-protect
        (progn
          (with-current-buffer indirect
            (setq-local clutch-connection 'edit-conn)
            (insert "SELECT 41"))
          (switch-to-buffer code)
          (switch-to-buffer indirect)
          (cl-letf (((symbol-function 'clutch--execute)
                     (lambda (sql &rest _)
                       (setq ran-in (list (current-buffer) sql)))))
            (with-current-buffer indirect
              (clutch-indirect-execute)))
          (should (buffer-live-p indirect))
          (should (equal ran-in (list indirect "SELECT 41"))))
      (dolist (buffer (list code indirect))
        (when (buffer-live-p buffer)
          (kill-buffer buffer))))))

(ert-deftest clutch-test-statement-reply-after-its-buffer-moved-is-only-reported ()
  "A statement's reply after its buffer left its connection should only be reported.
Drawing it put the old connection's error page in the result buffer that the
buffer's new connection names, and bound that buffer to the old connection."
  (with-temp-buffer
    (setq-local clutch-connection 'conn-a)
    (clutch-test--with-async-statements finishes
      (let ((a-alive t) rendered messages)
        (cl-letf (((symbol-function 'clutch--connection-key)
                   (lambda (conn) (if conn (symbol-name conn) "none")))
                  ((symbol-function 'clutch--connection-alive-p)
                   (lambda (conn) (or a-alive (not (eq conn 'conn-a)))))
                  ((symbol-function 'clutch--retire-query-connection) #'ignore)
                  ((symbol-function 'clutch-result--display-error)
                   (lambda (&rest _) (setq rendered t) nil))
                  ((symbol-function 'message)
                   (lambda (format-string &rest args)
                     (push (apply #'format format-string args) messages))))
          (clutch--execute "UPDATE t SET n = 1 WHERE id = 1")
          ;; Disconnected and connected elsewhere while the reply waits.
          (setq a-alive nil)
          (setq-local clutch-connection 'conn-b)
          (funcall (cdar finishes) nil
                   '(clutch-db-error
                     "Disconnected while the statement ran; its outcome is unknown"))
          (ert-run-idle-timers)
          (should-not rendered)
          (should (cl-some (lambda (text) (string-match-p "outcome is unknown" text))
                           messages))
          (should-not (clutch-db--foreground-busy-p 'conn-a)))))))

(ert-deftest clutch-test-redis-select-moves-the-session-to-its-database ()
  "A Redis SELECT should move the session to the database it selects.
A reconnect selected the database the connection was opened with, and the
cached keys and their metadata stayed that database's."
  (require 'redis)
  (require 'clutch-redis)
  (with-temp-buffer
    ;; The client reports the database that the SELECT moves it to, as
    ;; redis.el does once the server accepts it.
    (let ((conn (make-clutch-redis-conn :client (make-redis-conn :database "1")))
          primed)
      (setq-local clutch-connection conn)
      (setq-local clutch--connection-params '(:backend redis :database 0))
      (clutch-test--with-async-statements finishes
        (let ((clutch--schema-cache (make-hash-table :test 'eq)))
          (puthash conn 'keys-of-database-0 clutch--schema-cache)
          (cl-letf (((symbol-function 'clutch-result--display-select) #'ignore)
                    ((symbol-function 'clutch--prime-schema-cache)
                     (lambda (connection) (setq primed connection))))
            (clutch--execute "SELECT 1")
            (funcall (cdar finishes)
                     (make-clutch-db-result :columns '((:name "value"))
                                            :rows '(("OK")))
                     nil)
            (ert-run-idle-timers)
            (should (equal (plist-get clutch--connection-params :database) "1"))
            (should-not (gethash conn clutch--schema-cache))
            (should (eq primed conn))))))))

(ert-deftest clutch-test-repl-reply-after-the-repl-moved-draws-no-result ()
  "A REPL statement's reply after the REPL left its connection draws no result.
Showing the SELECT put the old connection's rows in the result buffer that
the REPL's new connection names, bound to the old connection."
  (with-temp-buffer
    (setq-local clutch-connection 'conn-a)
    (clutch-test--with-async-statements finishes
      (let (displayed output)
        (cl-letf (((symbol-function 'clutch--connection-key)
                   (lambda (conn) (if conn (symbol-name conn) "none")))
                  ((symbol-function 'clutch-result--display-select)
                   (lambda (&rest _) (setq displayed t)))
                  ((symbol-function 'clutch-repl--output)
                   (lambda (text) (push text output)))
                  ((symbol-function 'message) #'ignore))
          (clutch-repl--execute-and-print "SELECT 1")
          (setq-local clutch-connection 'conn-b)
          (funcall (cdar finishes)
                   (make-clutch-db-result :columns '((:name "1")) :rows '((1)))
                   nil)
          (ert-run-idle-timers)
          (should-not displayed)
          (should (string-match-p "not shown" (car output))))))))

(ert-deftest clutch-test-async-execute-presents-after-completion ()
  "An asynchronous statement should hold its connection until it finishes."
  (with-temp-buffer
    (insert "UPDATE t SET n = 1 WHERE id = 1")
    (setq-local clutch-connection 'async-conn)
    (clutch-test--with-async-statements finishes
      (let (displayed)
        (cl-letf (((symbol-function 'clutch-result--display)
                   (lambda (result _sql _elapsed) (setq displayed result))))
          (clutch--execute-and-mark (buffer-string) (point-min) (point-max))
          (should (equal (mapcar #'car finishes) (list (buffer-string))))
          (should (clutch-db--foreground-busy-p 'async-conn))
          (should (string-prefix-p
                   "Running"
                   (overlay-get clutch--executed-sql-overlay 'help-echo)))
          (should-error (clutch--execute "SELECT 2") :type 'user-error)
          (funcall (cdar finishes) (make-clutch-db-result :affected-rows 1) nil)
          (should-not displayed)
          (ert-run-idle-timers)
          (should (= (clutch-db-result-affected-rows displayed) 1))
          (should-not (gethash 'async-conn clutch--running-queries))
          (should-not (clutch-db--foreground-busy-p 'async-conn))
          (should-not clutch--execution-start-time)
          (should (string-prefix-p
                   "Last executed"
                   (overlay-get clutch--executed-sql-overlay 'help-echo))))))))

(ert-deftest clutch-test-page-load-keeps-result-until-it-succeeds ()
  "A page load should leave the page, its sort and staging until it succeeds.
It runs without blocking, staging waits for it, and a failure leaves the
result as it was."
  (clutch-test--with-result-state
      (:columns '("id" "name")
       :rows '((1 "a") (2 "b"))
       :connection 'async-conn
       :base-query "SELECT id, name FROM t"
       :server-pageable t
       :server-rewritable t)
    (clutch-test--with-async-statements finishes
      (let (messages)
        (cl-letf (((symbol-function 'clutch--ensure-connection) #'ignore)
                  ((symbol-function 'clutch-db-sql-surface-p) (lambda (&rest _) t))
                  ((symbol-function 'clutch-db-build-paged-sql)
                   (lambda (&rest _) "SELECT id, name FROM t ORDER BY name DESC"))
                  ((symbol-function 'clutch--refresh-display) #'ignore)
                  ((symbol-function 'message)
                   (lambda (format-string &rest args)
                     (push (apply #'format format-string args) messages))))
          (clutch-result--sort "name" t)
          (should (= (length finishes) 1))
          (should-not clutch--sort-column)
          (should-not clutch--order-by)
          (should (equal clutch--result-rows '((1 "a") (2 "b"))))
          (should (string-match-p
                   "A query is running"
                   (error-message-string
                    (should-error (clutch-edit--require-sql-staged-mutation
                                   "Edit / re-edit")
                                  :type 'user-error))))
          ;; An edit opened before the page load cannot be staged either.
          (should (string-match-p
                   "A query is running"
                   (error-message-string
                    (should-error (clutch-result--apply-edit
                                   0 1 "z" (list :identity [1] :original "a"
                                                 :original-state (cons nil "a")))
                                  :type 'user-error))))
          (funcall (cdar finishes) nil '(clutch-db-error "relation does not exist"))
          (ert-run-idle-timers)
          (should-not clutch--sort-column)
          (should-not clutch--order-by)
          (should (equal clutch--result-rows '((1 "a") (2 "b"))))
          (should (string-suffix-p "(result unchanged)" (car messages)))
          (clutch-result--sort "name" t)
          (funcall (cdar finishes)
                   (make-clutch-db-result :columns clutch--result-column-defs
                                          :rows '((2 "b") (1 "a")))
                   nil)
          (ert-run-idle-timers)
          (should (equal clutch--sort-column "name"))
          (should (equal clutch--order-by '("name" . "DESC")))
          (should (equal clutch--result-rows '((2 "b") (1 "a"))))
          (should (equal (car messages) "Sorted by name DESC")))))))

(ert-deftest clutch-test-page-load-leaves-a-result-of-another-connection ()
  "A page that arrives once its buffer shows another connection's result is dropped.
Showing it there would pair one database's rows with another's connection,
and an edit would then change the other database by the first one's keys.
A failure is dropped too: its error page would replace that result and bind
the buffer to the closed connection."
  (dolist (reply (list (list (make-clutch-db-result :rows '((3) (4))) nil)
                       (list nil '(clutch-db-error "connection closed"))))
    (clutch-test--with-result-state
        (:columns '("id") :rows '((1) (2)) :connection 'async-conn
         :base-query "SELECT id FROM t" :server-pageable t :result-max-rows 2)
      (clutch-test--with-async-statements finishes
        (let (shown)
          (cl-letf (((symbol-function 'clutch--ensure-connection) #'ignore)
                    ((symbol-function 'clutch-db-build-paged-sql)
                     (lambda (&rest _) "SELECT id FROM t PAGE 1"))
                    ((symbol-function 'clutch--connection-alive-p)
                     (lambda (conn) (not (eq conn 'async-conn))))
                    ((symbol-function 'clutch--show-execution-error)
                     (lambda (&rest _) (setq shown t) "failed"))
                    ((symbol-function 'clutch--refresh-display) #'ignore)
                    ((symbol-function 'message) #'ignore))
            (clutch-result--execute-page 1)
            (apply (cdar finishes) reply)
            ;; Before the reply is handled, the buffer shows another result.
            (setq-local clutch-connection 'other-conn
                        clutch--result-rows '((10) (20)))
            (ert-run-idle-timers)
            (should-not shown)
            (should (equal clutch--result-rows '((10) (20))))
            (should (eq clutch-connection 'other-conn))))))))

(ert-deftest clutch-test-query-activity-reply-needs-its-buffer-on-its-connection ()
  "A reply should reach its handler only while its buffer holds its connection.
That is the connection the reply came from, which an idle reconnect puts in
the buffer in place of the reserved one.  A buffer that holds another
connection, or none, ends the activity and calls MOVED; a killed buffer ends
it and says so; a handler that exits nonlocally ends it too."
  (let ((clutch-db--foreground-connections (make-hash-table :test 'eq))
        messages)
    (cl-letf (((symbol-function 'clutch--update-mode-line) #'ignore)
              ((symbol-function 'clutch--execution-refresh-start) #'ignore)
              ((symbol-function 'message)
               (lambda (format-string &rest args)
                 (push (apply #'format format-string args) messages))))
      (cl-flet ((reply (buffer-holds reply-from &optional kill handle)
                  (let ((buffer (generate-new-buffer " *clutch-reply*"))
                        activity events)
                    (with-current-buffer buffer
                      (setq-local clutch-connection 'old-conn)
                      (setq activity (clutch--begin-query-activity 'old-conn))
                      (setq-local clutch-connection buffer-holds))
                    (when kill
                      (kill-buffer buffer))
                    (condition-case nil
                        (clutch--query-activity-reply
                         activity reply-from
                         (or handle (lambda () (push 'handled events)))
                         :moved (lambda () (push 'moved events)))
                      (error (push 'signalled events)))
                    (when (buffer-live-p buffer)
                      (kill-buffer buffer))
                    (list (nreverse events)
                          (and (plist-get activity :ended) t)))))
        (should (equal (reply 'old-conn 'old-conn) '((handled) nil)))
        (should (equal (reply 'new-conn 'new-conn) '((handled) nil)))
        (should (equal (reply 'other-conn 'old-conn) '((moved) t)))
        (should (equal (reply nil 'old-conn) '((moved) t)))
        (should (equal (reply 'old-conn 'old-conn t) '(nil t)))
        (should (member "Query finished after its buffer was killed" messages))
        (should (equal (reply 'old-conn 'old-conn nil
                              (lambda () (error "Boom")))
                       '((signalled) t)))))))

(ert-deftest clutch-test-query-activity-end-keeps-a-later-ones-time ()
  "Ending a query activity should leave the time of a later one in its buffer.
A page load that ends after its buffer moved to another connection and
started loading a page there cleared the time of that running load."
  (clutch-test--with-result-state
      (:columns '("id") :rows '((1) (2)) :connection 'async-conn
       :base-query "SELECT id FROM t" :server-pageable t :result-max-rows 2)
    (clutch-test--with-async-statements finishes
      (cl-letf (((symbol-function 'clutch--ensure-connection) #'ignore)
                ((symbol-function 'clutch-db-build-paged-sql)
                 (lambda (&rest _) "SELECT id FROM t PAGE 1"))
                ((symbol-function 'clutch--refresh-display) #'ignore)
                ((symbol-function 'message) #'ignore))
        (clutch-result--execute-page 1)
        (funcall (cdar finishes) nil '(clutch-db-error "connection closed"))
        (setq-local clutch-connection 'other-conn)
        (clutch-result--execute-page 1)
        (ert-run-idle-timers)
        (should (gethash 'other-conn clutch--running-queries))
        (should clutch--execution-start-time)
        (funcall (cdar finishes)
                 (make-clutch-db-result :columns clutch--result-column-defs
                                        :rows '((3) (4)))
                 nil)
        (ert-run-idle-timers)
        (should-not clutch--execution-start-time)))))

(ert-deftest clutch-test-last-page-stops-when-its-count-is-cancelled ()
  "C-g during the count of a last-page move should stop the page load.
The cancel may meet a count that has already finished; the move stops
there, and the result keeps its rows and its unknown total."
  (clutch-test--with-result-state
      (:columns '("id") :rows '((1) (2)) :connection 'async-conn
       :base-query "SELECT id FROM t" :server-pageable t :server-rewritable t
       :page-total-rows nil :result-max-rows 2)
    (clutch-test--with-async-statements finishes
      (cl-letf (((symbol-function 'clutch--ensure-connection) #'ignore)
                ((symbol-function 'clutch-db-build-count-sql)
                 (lambda (&rest _) "SELECT count(*) FROM t"))
                ((symbol-function 'clutch-db-build-paged-sql)
                 (lambda (_conn _sql page-num &rest _)
                   (format "SELECT id FROM t PAGE %d" page-num)))
                ((symbol-function 'clutch-db-interrupt-query) (lambda (_conn) t))
                ((symbol-function 'clutch--refresh-display) #'ignore)
                ((symbol-function 'clutch--refresh-footer-line) #'ignore)
                ((symbol-function 'message) #'ignore))
        (clutch-result-last-page)
        (clutch-cancel-query-or-quit)
        (funcall (cdar finishes) (make-clutch-db-result :rows '((5))) nil)
        (ert-run-idle-timers)
        (should (equal (mapcar #'car finishes) '("SELECT count(*) FROM t")))
        (should-not clutch--page-total-rows)
        (should (equal clutch--result-rows '((1) (2))))
        (should-not (clutch-db--foreground-busy-p 'async-conn))))))

(ert-deftest clutch-test-last-page-counts-rows-first-without-blocking ()
  "The last page of an uncounted result should load once the count arrives."
  (clutch-test--with-result-state
      (:columns '("id")
       :rows '((1) (2))
       :connection 'async-conn
       :base-query "SELECT id FROM t"
       :server-pageable t
       :server-rewritable t
       :page-total-rows nil
       :result-max-rows 2)
    (clutch-test--with-async-statements finishes
      (cl-letf (((symbol-function 'clutch--ensure-connection) #'ignore)
                ((symbol-function 'clutch-db-build-count-sql)
                 (lambda (&rest _) "SELECT count(*) FROM t"))
                ((symbol-function 'clutch-db-build-paged-sql)
                 (lambda (_conn _sql page-num &rest _)
                   (format "SELECT id FROM t PAGE %d" page-num)))
                ((symbol-function 'clutch--refresh-display) #'ignore)
                ((symbol-function 'clutch--refresh-footer-line) #'ignore)
                ((symbol-function 'message) #'ignore))
        (clutch-result-last-page)
        (should (equal (mapcar #'car finishes) '("SELECT count(*) FROM t")))
        (funcall (cdar finishes) (make-clutch-db-result :rows '((5))) nil)
        (ert-run-idle-timers)
        (should (= clutch--page-total-rows 5))
        (should (equal (mapcar #'car finishes)
                       '("SELECT id FROM t PAGE 2" "SELECT count(*) FROM t")))
        ;; The page load counts its own time once the count's has ended.
        (should clutch--execution-start-time)))))

(ert-deftest clutch-test-async-execute-drops-outcome-of-killed-buffer ()
  "A statement finishing after its buffer is killed should only be reported."
  (let ((buffer (generate-new-buffer " *clutch-async-source*"))
        messages displayed)
    (clutch-test--with-async-statements finishes
      (cl-letf (((symbol-function 'clutch-result--display)
                 (lambda (&rest _args) (setq displayed t)))
                ((symbol-function 'message)
                 (lambda (format-string &rest args)
                   (push (apply #'format format-string args) messages))))
        (with-current-buffer buffer
          (setq-local clutch-connection 'async-conn)
          (clutch--execute "UPDATE t SET n = 1 WHERE id = 1"))
        (kill-buffer buffer)
        (funcall (cdar finishes) (make-clutch-db-result :affected-rows 1) nil)
        (ert-run-idle-timers)
        (should-not displayed)
        (should (member "Query finished after its buffer was killed" messages))
        (should-not (gethash 'async-conn clutch--running-queries))
        (should-not (clutch-db--foreground-busy-p 'async-conn))))))

(ert-deftest clutch-test-async-markers-follow-edits-while-running ()
  "Status markers should stay on their statement while the buffer is edited."
  (cl-flet ((marker-line ()
              (save-excursion
                (goto-char (overlay-start clutch--executed-sql-overlay))
                (buffer-substring-no-properties
                 (line-beginning-position) (line-end-position)))))
    (with-temp-buffer
      (insert "SELECT 1;\nUPDATE t SET n = 1;\n")
      (setq-local clutch-connection 'async-conn)
      (clutch-test--with-async-statements finishes
        (cl-letf (((symbol-function 'clutch-result--display) #'ignore)
                  ((symbol-function 'clutch-db-interrupt-query) (lambda (_conn) t)))
          (clutch--execute-and-mark "UPDATE t SET n = 1;" 11 (point-max))
          (goto-char (point-min))
          (insert "-- typed while it runs\n")
          (clutch-cancel-query-or-quit)
          (should (equal (marker-line) "UPDATE t SET n = 1;"))
          (funcall (cdar finishes) (make-clutch-db-result :affected-rows 1) nil)
          (ert-run-idle-timers)
          (should (equal (marker-line) "UPDATE t SET n = 1;")))))
    (with-temp-buffer
      (insert "SELECT 1;  \nUPDATE t SET n = 1;\n")
      (setq-local clutch-connection 'async-conn)
      (goto-char (point-min))
      (search-forward "UPDATE")
      (clutch-test--with-async-statements finishes
        (cl-letf (((symbol-function 'clutch-result--display) #'ignore))
          (clutch-execute-dwim (point) (point))
          (goto-char (point-min))
          (end-of-line)
          (insert "-- typed while it runs")
          (funcall (cdar finishes) (make-clutch-db-result :affected-rows 1) nil)
          (ert-run-idle-timers)
          (should (equal (marker-line) "UPDATE t SET n = 1;")))))
    (with-temp-buffer
      (insert "UPDATE a SET n = 1;\nUPDATE b SET n = 2;\n")
      (setq-local clutch-connection 'async-conn)
      (clutch-test--with-async-statements finishes
        (cl-letf (((symbol-function 'clutch-result--display) #'ignore)
                  ((symbol-function 'message) #'ignore))
          (clutch--execute-statements
           (clutch--split-statement-specs (buffer-string) (point-min)))
          (goto-char (point-min))
          (insert "-- typed while the first one runs\n")
          (funcall (cdar finishes) (make-clutch-db-result :affected-rows 1) nil)
          (ert-run-idle-timers)
          (funcall (cdar finishes) (make-clutch-db-result :affected-rows 1) nil)
          (ert-run-idle-timers)
          (should (equal (marker-line) "UPDATE b SET n = 2;")))))))

(ert-deftest clutch-test-async-statements-run-one-after-another ()
  "A batch should send each statement after the previous one finishes."
  (with-temp-buffer
    (setq-local clutch-connection 'async-conn)
    (clutch-test--with-async-statements finishes
      (let (messages)
        (cl-letf (((symbol-function 'clutch--show-execution-error)
                   (lambda (&rest _args) "duplicate key"))
                  ((symbol-function 'message)
                   (lambda (format-string &rest args)
                     (push (apply #'format format-string args) messages))))
          (clutch--execute-statements
           '("INSERT INTO t VALUES (1)"
             "INSERT INTO t VALUES (2)"
             "INSERT INTO t VALUES (3)"))
          (should (equal (mapcar #'car finishes) '("INSERT INTO t VALUES (1)")))
          (funcall (cdar finishes) (make-clutch-db-result :affected-rows 1) nil)
          (ert-run-idle-timers)
          (should (equal (mapcar #'car finishes)
                         '("INSERT INTO t VALUES (2)"
                           "INSERT INTO t VALUES (1)")))
          (funcall (cdar finishes) nil '(clutch-db-error "duplicate key"))
          (ert-run-idle-timers)
          (should (= (length finishes) 2))
          (should (cl-find-if (lambda (text)
                                (string-prefix-p
                                 "Statement 2 failed: duplicate key" text))
                              messages))
          (should-not (clutch-db--foreground-busy-p 'async-conn)))))))

(ert-deftest clutch-test-async-batch-stops-when-cancel-meets-a-finished-statement ()
  "C-g should stop a batch even when its statement finishes first.
A statement's result can arrive before the cancel; that statement keeps
its outcome and the next one does not run."
  (with-temp-buffer
    (setq-local clutch-connection 'async-conn)
    (clutch-test--with-async-statements finishes
      (let (messages)
        (cl-letf (((symbol-function 'clutch-db-interrupt-query)
                   (lambda (_conn) t))
                  ((symbol-function 'message)
                   (lambda (format-string &rest args)
                     (push (apply #'format format-string args) messages))))
          (clutch--execute-statements
           '("UPDATE t SET n = 1 WHERE id = 1"
             "DELETE FROM t WHERE id = 2"))
          (clutch-cancel-query-or-quit)
          (funcall (cdar finishes) (make-clutch-db-result :affected-rows 1) nil)
          (ert-run-idle-timers)
          (should (equal (mapcar #'car finishes)
                         '("UPDATE t SET n = 1 WHERE id = 1")))
          (should (member "1 statement executed, then cancelled" messages))
          (should-not (clutch-db--foreground-busy-p 'async-conn)))))))

(ert-deftest clutch-test-async-batch-releases-its-connection-on-a-quit ()
  "A quit while a batch starts its next statement should end the batch.
Otherwise the connection stays reserved and the mode line keeps counting."
  (with-temp-buffer
    (setq-local clutch-connection 'async-conn)
    (clutch-test--with-async-statements finishes
      (let ((execute (symbol-function 'clutch--execute-statement))
            (calls 0))
        (cl-letf (((symbol-function 'message) #'ignore)
                  ((symbol-function 'clutch--execute-statement)
                   (lambda (&rest args)
                     (if (= (cl-incf calls) 2)
                         (signal 'quit nil)
                       (apply execute args)))))
          (clutch--execute-statements
           '("UPDATE t SET n = 1 WHERE id = 1" "UPDATE t SET n = 2 WHERE id = 2"))
          (funcall (cdar finishes) (make-clutch-db-result :affected-rows 1) nil)
          ;; ERT does not fail a test that quits, so take the quit here.
          (condition-case nil (ert-run-idle-timers) (quit nil))
          (should (= calls 2))
          (should-not (clutch-db--foreground-busy-p 'async-conn)))))))

(ert-deftest clutch-test-async-batch-failure-after-buffer-kill-says-nothing-odd ()
  "A batch statement that fails after its buffer is killed should not say nil."
  (let ((source (generate-new-buffer " *clutch-batch-killed*"))
        messages)
    (clutch-test--with-async-statements finishes
      (cl-letf (((symbol-function 'message)
                 (lambda (format-string &rest args)
                   (push (apply #'format format-string args) messages))))
        (with-current-buffer source
          (setq-local clutch-connection 'async-conn)
          (clutch--execute-statements
           '("UPDATE t SET n = 1 WHERE id = 1" "UPDATE t SET n = 2 WHERE id = 2")))
        (kill-buffer source)
        (funcall (cdar finishes) nil '(clutch-db-error "connection reset"))
        (ert-run-idle-timers)
        (should-not (member "nil" messages))
        (should-not (clutch-db--foreground-busy-p 'async-conn))))))

(ert-deftest clutch-test-cancel-command-cancels-a-running-query-or-quits ()
  "C-g should ask once to cancel the running query and otherwise quit."
  (with-temp-buffer
    (setq-local clutch-connection 'async-conn)
    (let ((clutch--running-queries (make-hash-table :test 'eq))
          (accepted t)
          interrupts)
      (cl-letf (((symbol-function 'clutch--update-mode-line) #'ignore)
                ((symbol-function 'message) #'ignore)
                ((symbol-function 'clutch-db-interrupt-query)
                 (lambda (conn) (push conn interrupts) accepted)))
        (cl-flet ((press ()
                    (condition-case nil
                        (progn (clutch-cancel-query-or-quit) 'cancelled)
                      (quit 'quit))))
          (should (eq (press) 'quit))
          (puthash 'async-conn
                   (list :buffer (current-buffer) :region nil :cancelling nil)
                   clutch--running-queries)
          (setq accepted nil)
          (should (eq (press) 'cancelled))
          (should-not (plist-get (gethash 'async-conn clutch--running-queries)
                                 :cancelling))
          (setq accepted t)
          (should (eq (press) 'cancelled))
          (should (plist-get (gethash 'async-conn clutch--running-queries)
                             :cancelling))
          (should (eq (press) 'quit))
          (should (equal interrupts '(async-conn async-conn))))))))

(ert-deftest clutch-test-present-outcome-uses-executing-connection ()
  "Presentation should use the connection that produced the outcome."
  (let ((result (make-clutch-db-result :columns ["id"] :rows '((1))))
        displayed-connection)
    (cl-letf (((symbol-function 'clutch-result--display-select)
               (lambda (connection &rest _args)
                 (setq displayed-connection connection))))
      (clutch--present-statement-outcome
       "SELECT 1" 'old-conn
       (list :connection 'new-conn
             :result result
             :result-query-p t
             :source-buffer (current-buffer)))
      (should (eq displayed-connection 'new-conn)))))

(ert-deftest clutch-test-handle-query-quit-remembers-interrupt-error-details-and-debug-event ()
  "Interrupt RPC failures should record details and retain reconnect anchors."
  (with-temp-buffer
    (let* ((conn (make-clutch-db-sqlite-conn :database "/tmp/debug.db"))
           (clutch-debug-mode t)
           (clutch-connection conn)
           (raw-message "Connection refused (host=db.example.com, port=3306)")
           (captured-message nil)
           (disconnected nil)
           (live t)
           (record (generate-new-buffer " *clutch-abandoned-record*")))
      (unwind-protect
          (progn
            (clutch--clear-debug-capture)
            (with-current-buffer record
              (clutch-record-mode)
              (setq-local clutch-connection conn
                          clutch--connection-render-state
                          '(:connected-p t)))
            (cl-letf (((symbol-function 'clutch-db-backend-key)
                      (lambda (_conn) 'pg))
                      ((symbol-function 'clutch--connection-alive-p)
                       (lambda (_conn) live))
                      ((symbol-function 'clutch-db-interrupt-query)
                       (lambda (_conn)
                         (signal 'clutch-db-error (list raw-message))))
                      ((symbol-function 'clutch-db-disconnect)
                       (lambda (_conn)
                         (setq disconnected t
                               live nil)))
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
                         (clutch--humanize-db-error
                          (error-message-string err)))))
                     (details clutch--buffer-error-details)
                     (diag (plist-get details :diag))
                     (context (plist-get diag :context))
                     (debug-text (clutch-test--debug-buffer-string)))
                (should disconnected)
                (should details)
                (should (eq (plist-get details :backend) 'pg))
                (should (equal (plist-get details :summary) summary))
                (should (equal (plist-get diag :raw-message) raw-message))
                (should (plist-member context :sql))
                (should-not (plist-get context :sql))
                (should
                 (equal captured-message
                        (format
                         "Interrupt failed: %s"
                         (clutch--debug-workflow-message message-summary))))
                (dolist (expected
                         `(,(concat "Operation: cancel\nPhase: error")
                           ,(concat "Summary: " message-summary)
                           "Operation: interrupt\nPhase: disconnect"))
                  (should (string-match-p (regexp-quote expected) debug-text)))
                (with-current-buffer record
                  (should (eq clutch-connection conn))
                  (should
                   (string-match-p
                    "DISCONNECTED"
                    (substring-no-properties
                     (clutch--header-with-disconnect-badge "Record"))))))))
        (when (buffer-live-p record)
          (kill-buffer record))))))

(ert-deftest clutch-test-execute-uses-backend-result-query-p ()
  "Execute should let the backend classify non-SQL result-set queries."
  (let (captured outcome)
    (cl-letf (((symbol-function 'clutch--confirm-query-execution) #'ignore)
              ((symbol-function 'clutch-db-result-query-p)
               (lambda (conn sql)
                 (setq captured (list conn sql))
                 t))
              ((symbol-function 'clutch--run-db-query)
               (lambda (&rest _) (make-clutch-db-result))))
      (setq outcome
            (clutch-test--await-outcome
             (lambda (k)
               (clutch--execute-statement
                "db.users.find()" 'document-conn nil nil k))))
      (should (equal captured '(document-conn "db.users.find()")))
      (should (plist-get outcome :result-query-p)))))

(ert-deftest clutch-test-execute-statements-confirms-each-nonselect ()
  "Batch execution should apply destructive and high-risk guards."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn)
    (let (queries risky-sqls yes-prompts)
      (cl-letf (((symbol-function 'clutch--run-db-query)
                 (lambda (_conn sql)
                   (push sql queries)
                   (make-clutch-db-result :affected-rows 1)))
                ((symbol-function 'clutch--confirm-high-risk-query)
                 (lambda (sql)
                   (push sql risky-sqls)
                   (string-prefix-p "UPDATE" sql)))
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
                ((symbol-function 'clutch--confirm-high-risk-query)
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

(ert-deftest clutch-test-clone-preserves-qualified-relation-and-values ()
  "Clone/import/stage/execute preserves the source schema and submitted values."
  (require 'clutch-db-sqlite)
  (dolist (value '("NULL" "null" "" nil))
    (dolist (import '(nil "payload\nreplacement\n"))
      (let ((conn (clutch-db-connect 'sqlite '(:database ":memory:")))
            result-buf insert-buf)
        (unwind-protect
            (progn
              (dolist (sql '("ATTACH DATABASE ':memory:' AS other"
                             "CREATE TABLE main.audit (id INTEGER PRIMARY KEY, payload TEXT DEFAULT 'fallback')"
                             "CREATE TABLE other.audit (id INTEGER PRIMARY KEY, payload TEXT DEFAULT 'fallback')"))
                (clutch-db-query conn sql))
              (clutch-db-execute-params
               conn "INSERT INTO other.audit (payload) VALUES (?)" (list value))
              (with-temp-buffer
                (clutch-mode)
                (setq-local clutch-connection conn)
                (let ((source (current-buffer)))
                  (clutch-test--execute-and-present "SELECT * FROM other.audit" conn)
                  (setq result-buf
                        (buffer-local-value 'clutch--last-result-buffer source)))
                (with-current-buffer result-buf
                  (clutch--goto-cell 0 0)
                  (clutch-clone-row-to-insert))
                (setq insert-buf (get-buffer "*clutch-insert: audit*"))
                (with-current-buffer insert-buf
                  (when import (clutch-result-insert-import-delimited import))
                  (clutch-result-insert-stage))
                (with-current-buffer result-buf
                  (clutch-result--submit-mutation-batch
                   (clutch-result--build-pending-insert-statements) nil nil)
                  (setq clutch--pending-inserts nil)))
              (should (equal (clutch-db-result-rows
                              (clutch-db-query conn "SELECT payload FROM other.audit ORDER BY id"))
                             (list (list value) (list (if import "replacement" value)))))
              (should-not (clutch-db-result-rows
                           (clutch-db-query conn "SELECT * FROM main.audit"))))
          (dolist (buf (list insert-buf result-buf))
            (when (buffer-live-p buf) (kill-buffer buf)))
          (clutch-db-disconnect conn))))))

(ert-deftest clutch-test-qualified-sqlite-result-changes-its-own-table ()
  "Every change staged on a result of aux.people should go to aux.people.
Its key and columns come from aux.people, not from main.people, which
SQLite finds first for the bare name, has another key and lacks a column."
  (require 'clutch-db-sqlite)
  (let ((conn (clutch-db-connect 'sqlite '(:database ":memory:")))
        result-buf)
    (unwind-protect
        (progn
          (dolist (sql '("CREATE TABLE main.people (id TEXT PRIMARY KEY, name TEXT)"
                         "ATTACH DATABASE ':memory:' AS aux"
                         "CREATE TABLE aux.people (pk INTEGER PRIMARY KEY, id TEXT, name TEXT, nick TEXT)"
                         "INSERT INTO aux.people VALUES (1, 'same', 'Ann', 'a'), (2, 'same', 'Bob', 'b')"))
            (clutch-db-query conn sql))
          (with-temp-buffer
            (clutch-mode)
            (setq-local clutch-connection conn)
            (let ((source (current-buffer)))
              (clutch-test--execute-and-present
               "SELECT * FROM aux.people ORDER BY pk" conn)
              (setq result-buf
                    (buffer-local-value 'clutch--last-result-buffer source))))
          (with-current-buffer result-buf
            (should (equal (plist-get clutch--row-identity :columns) '("pk")))
            (let ((rows clutch--result-rows))
              (clutch-result--apply-edit
               0 (cl-position "nick" clutch--result-columns :test #'string=) "A"
               (list :identity (clutch-db-row-identity-values
                                (nth 0 rows) clutch--row-identity)
                     :original "a"
                     :original-state (cons nil "a")))
              (setq-local clutch--pending-deletes
                          (list (clutch-db-row-identity-values
                                 (nth 1 rows) clutch--row-identity))
                          clutch--pending-inserts
                          '((("pk" . "3") ("id" . "x") ("name" . "Cy") ("nick" . "c")))))
            (should (equal (clutch-result--pending-sql-statements)
                           '("INSERT INTO aux.people (\"pk\", \"id\", \"name\", \"nick\") VALUES ('3', 'x', 'Cy', 'c')"
                             "UPDATE aux.people SET \"nick\" = 'A' WHERE \"pk\" = 1"
                             "DELETE FROM aux.people WHERE \"pk\" = 2")))
            (cl-letf (((symbol-function 'yes-or-no-p) (lambda (&rest _) t)))
              (clutch-result-submit)
              (clutch-test--await-queries)))
          (should (equal (clutch-db-result-rows
                          (clutch-db-query conn "SELECT * FROM aux.people ORDER BY pk"))
                         '((1 "same" "Ann" "A") (3 "x" "Cy" "c"))))
          (should-not (clutch-db-result-rows
                       (clutch-db-query conn "SELECT * FROM main.people"))))
      (when (buffer-live-p result-buf)
        (kill-buffer result-buf))
      (clutch-db-disconnect conn))))

(ert-deftest clutch-test-qualified-sqlite-result-follows-keys-in-its-schema ()
  "Following a foreign key of aux.children should open aux.parents.
SQLite finds main.parents first for the bare name, with a row of the same key."
  (require 'clutch-db-sqlite)
  (let ((conn (clutch-db-connect 'sqlite '(:database ":memory:")))
        result-buf followed)
    (unwind-protect
        (progn
          (dolist (sql '("CREATE TABLE main.parents (id INTEGER PRIMARY KEY, label TEXT)"
                         "INSERT INTO main.parents VALUES (1, 'main')"
                         "ATTACH DATABASE ':memory:' AS aux"
                         "CREATE TABLE aux.parents (id INTEGER PRIMARY KEY, label TEXT)"
                         "INSERT INTO aux.parents VALUES (1, 'aux')"
                         "CREATE TABLE aux.children (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parents(id))"
                         "INSERT INTO aux.children VALUES (1, 1)"))
            (clutch-db-query conn sql))
          (with-temp-buffer
            (clutch-mode)
            (setq-local clutch-connection conn)
            (let ((source (current-buffer)))
              (clutch-test--execute-and-present "SELECT * FROM aux.children" conn)
              (setq result-buf
                    (buffer-local-value 'clutch--last-result-buffer source))))
          (ert-run-idle-timers)
          (with-current-buffer result-buf
            (cl-letf (((symbol-function 'clutch--execute)
                       (lambda (sql &rest _) (setq followed sql))))
              (clutch-record--follow-fk
               (cdr (assq 1 clutch--fk-info)) 1 result-buf)))
          (should (equal followed
                         "SELECT * FROM \"aux\".\"parents\" WHERE \"id\" = 1"))
          (should (equal (clutch-db-result-rows (clutch-db-query conn followed))
                         '((1 "aux")))))
      (when (buffer-live-p result-buf)
        (kill-buffer result-buf))
      (clutch-db-disconnect conn))))

(ert-deftest clutch-test-json-cancel-retains-null-on-clone ()
  "Opening and cancelling a JSON editor preserves SQL NULL on submission."
  (let ((conn (clutch-db-connect 'sqlite '(:database ":memory:")))
        result-buf insert-buf json-buf)
    (unwind-protect
        (progn
          (clutch-db-query conn "CREATE TABLE items(id INTEGER PRIMARY KEY, name TEXT, payload JSON DEFAULT '{\"fallback\":true}')")
          (clutch-db-query conn "INSERT INTO items(name,payload) VALUES('kept',NULL)")
          (with-temp-buffer
            (clutch-mode)
            (setq-local clutch-connection conn)
            (let ((source (current-buffer)))
              (clutch-test--execute-and-present "SELECT * FROM items" conn)
              (setq result-buf (buffer-local-value 'clutch--last-result-buffer source)))
            (with-current-buffer result-buf
              (clutch--goto-cell 0 0)
              (clutch-clone-row-to-insert))
            (setq insert-buf (get-buffer "*clutch-insert: items*"))
            (with-current-buffer insert-buf
              (goto-char (plist-get (clutch-result-insert--field-state "payload") :value-marker))
              (should (equal (assoc "payload" (clutch-result-insert--parse-fields)) '("payload")))
              (setq json-buf (clutch-result-insert-edit-json-field)))
            (with-current-buffer json-buf (clutch-result-insert-json-cancel))
            (with-current-buffer insert-buf
              (clutch-result-insert-stage))
            (with-current-buffer result-buf
              (clutch-result--submit-mutation-batch
               (clutch-result--build-pending-insert-statements) nil nil)
              (setq clutch--pending-inserts nil)))
          (let ((rows (clutch-db-result-rows
                       (clutch-db-query conn "SELECT name,payload FROM items ORDER BY id"))))
            (should (equal rows '(("kept" nil) ("kept" nil))))))
      (dolist (buf (list json-buf insert-buf result-buf))
        (when (buffer-live-p buf) (kill-buffer buf)))
      (clutch-db-disconnect conn))))

(ert-deftest clutch-test-jdbc-preview-cannot-be-written-or-exported ()
  "A legacy agent's incomplete preview never becomes writable text."
  (require 'clutch-db-jdbc)
  (let* ((preview (make-string 256 ?a))
         (value (car (clutch-jdbc--normalize-row
                      (list (list :__type "clob" :length 1000 :preview preview)))))
         (clutch--result-columns '("body"))
         (clutch--result-column-defs '((:name "body" :type-category text))))
    (should-not (stringp value))
    (should (= (clutch-db-value-preview-length value) 1000))
    (should (equal (clutch-db-value-preview-text value) preview))
    (should (string-match-p "CLOB preview; 1000 total"
                            (plist-get (clutch--view-spec value nil) :content)))
    (should-error (clutch-result--editable-field-string
                   value '(:type-category text) '(:type "CLOB"))
                  :type 'user-error)
    (dolist (format '(csv tsv org-table insert))
      (should-error (clutch-result--copy-lines format (list (list value)) '(0))
                    :type 'user-error))
    (dolist (export '(clutch--export-csv-content clutch--export-tsv-content))
      (should-error (funcall export (list (list value))) :type 'user-error))))

(ert-deftest clutch-test-insert-explicit-values-survive-validation-and-reopen ()
  "NULL, empty text, default and literal text remain distinct in the form."
  (clutch-test--with-pop-to-buffer-capture insert-buf
    (clutch-test--with-insert-result-buffer result-buf
        (:columns '("payload")
                  :column-defs '((:name "payload" :type-category text))
                  :connection 'fake-conn :source-table "audit")
      (cl-letf (((symbol-function 'clutch--ensure-column-details)
                 (lambda (&rest _)
                   '((:name "payload" :type "text" :nullable t :default "fallback")))))
        (clutch-result-insert--open-buffer "audit" result-buf)
        (dolist (case '((clutch-result-insert-set-null (("payload")))
                        (clutch-result-insert-set-empty (("payload" . "")))
                        (clutch-result-insert-set-default nil)))
          (with-current-buffer insert-buf
            (clutch-test--goto-insert-field-value "payload")
            (funcall (car case))
            (clutch-result-insert--run-idle-validation insert-buf)
            (should (equal (clutch-result-insert--parse-fields) (cadr case))))
          (clutch-result-insert--open-buffer "audit" result-buf (cadr case))
          (with-current-buffer insert-buf
            (should (equal (clutch-result-insert--parse-fields) (cadr case)))
            (clutch-result-insert-import-delimited "payload\nimported\n")
            (should (equal (clutch-result-insert--parse-fields)
                           '(("payload" . "imported"))))
            (should-not (plist-get (clutch-result-insert--field-state "payload")
                                   :special-value))))
        (with-current-buffer insert-buf
          (clutch-test--goto-insert-field-value "payload")
          (clutch-result-insert-set-null)
          (insert "NULL")
          (should (equal (clutch-result-insert--parse-fields)
                         '(("payload" . "NULL")))))))))

(ert-deftest clutch-test-clone-skips-generated-previews-before-conversion ()
  "Omitted generated columns do not prevent cloning the writable fields."
  (let ((clutch-connection 'fake-conn)
        (clutch--result-columns '("id" "generated_body" "name"))
        (clutch--result-column-defs
         '((:name "id") (:name "generated_body") (:name "name")))
        (clutch--row-identity
         (clutch-test--primary-row-identity "audit" '("id") '(0))))
    (cl-letf (((symbol-function 'clutch--ensure-column-details)
               (lambda (&rest _)
                 '((:name "id" :primary-key t)
                   (:name "generated_body" :generated t)
                   (:name "name" :type "text")))))
      (should (equal (clutch-result-insert--clone-fields-from-row-values
                      "audit" (list 1 (make-clutch-db-value-preview
                                       :type 'clob :length 1000 :text "preview")
                                    "kept"))
                     '(("name" . "kept")))))))

(provide 'clutch-test)

;;; clutch-test.el ends here
