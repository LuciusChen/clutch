;;; clutch-test-ui.el --- Result rendering ERT tests for clutch -*- lexical-binding: t; -*-

;;; Commentary:

;; Value formatting, padding, column layout, row and separator rendering,
;; header line and footer, custom column displayers, cell preview and
;; temporal formatting tests for clutch.

;;; Code:

(eval-and-compile
  (require 'clutch-test-common)
  (require 'clutch-db-sqlite))

;;;; Rendering — value formatting

(ert-deftest clutch-test-format-value-values ()
  "Result values should render as compact display strings."
  :tags '(:smoke)
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

(ert-deftest clutch-test-qualified-changes-preserve-target-and-default-syntax ()
  "Qualified targets and DEFAULT assignments should remain SQL syntax.
UPDATE, DELETE and INSERT all name APP.reports as the query wrote it,
although Oracle stores the table as REPORTS.  Without a row identity, an
INSERT after a server-side filter wrapped the query names the stored
table with its schema."
  (let ((clutch-connection
         (make-clutch-jdbc-conn :params '(:driver oracle)))
        (clutch--last-query "SELECT * FROM APP.reports")
        (clutch--result-columns '("ID" "STATUS" "NOTE"))
        (clutch--result-column-defs
         '((:name "ID" :backend-type "NUMBER" :source-column "ID")
           (:name "STATUS" :backend-type "VARCHAR2"
            :source-column "STATUS")
           (:name "NOTE" :backend-type "VARCHAR2" :source-column "NOTE")))
        (identity '(:kind primary-key :name "PRIMARY"
                    :table "REPORTS" :source-token "APP.reports"
                    :columns ("ID") :indices (0) :source-indices (0))))
    (cl-letf (((symbol-function 'clutch--connection-alive-p) #'always)
              ((symbol-function 'clutch--ensure-column-details)
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
        (should (string-prefix-p "DELETE FROM APP.reports WHERE" delete-sql))
        (should (equal (car (clutch-result-insert--build-sql
                             clutch-connection (clutch--table-key "REPORTS" "APP")
                             '(("ID" . "7") ("STATUS" . "new"))))
                       "INSERT INTO APP.reports (\"ID\", \"STATUS\") VALUES (?, ?)"))
        (let ((clutch--last-query
               "SELECT * FROM (SELECT * FROM APP.reports) _clutch_filter WHERE ID = 7"))
          (should (equal (car (clutch-result-insert--build-sql
                               clutch-connection (clutch--table-key "REPORTS" "APP")
                               '(("ID" . "7"))))
                         "INSERT INTO \"APP\".\"REPORTS\" (\"ID\") VALUES (?)")))))))

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
    (cl-letf (((symbol-function 'clutch--connection-alive-p) #'always)
              ((symbol-function 'clutch--ensure-column-details)
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
  "Mutation SQL should use canonical source names, including behind aliases."
  (pcase-dolist (`(,label ,driver ,table ,key ,column ,canonical ,absent)
                 '(("canonical source-column case" oracle "USERS" "ID"
                    (:name "name" :source-column "name")
                    (:name "NAME" :backend-type "VARCHAR2") nil)
                   ("source column behind an alias" jdbc "users" "id"
                    (:name "display_name" :source-column "NAME")
                    (:name "name" :backend-type "text")
                    ("display_name" "\"NAME\""))))
    (ert-info (label)
      (let ((clutch-connection
             (make-clutch-jdbc-conn :params (list :driver driver)))
            (clutch--result-columns (list (plist-get column :name)))
            (clutch--result-column-defs (list column))
            (identity (list :kind 'primary-key :name "PRIMARY"
                            :table table :columns (list key) :indices '(1))))
        (cl-letf (((symbol-function 'clutch--connection-alive-p) #'always)
                  ((symbol-function 'clutch--ensure-column-details)
                   (lambda (_conn _table &optional _strict) (list canonical))))
          (pcase-let ((`(,sql . ,_)
                       (clutch-result--build-update-stmt
                        table [7] '((0 . "Ada")) identity
                        (clutch-result--update-source-columns table '(0) "test"))))
            (should (string-search
                     (format "SET \"%s\" = ?" (plist-get canonical :name)) sql))
            (dolist (name absent)
              (should-not (string-search name sql)))))))))

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
  "SELECT display should install source metadata, errors, and window metrics."
  :tags '(:smoke)
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
  "Staging a Record field keeps point there after the editor window closes."
  (save-window-excursion
    (clutch-test--with-result-state-buffer result-buf
        (:connection (make-clutch-test-conn :table "users"
                                            :columns '((:name "note" :type "text")))
         :connection-params '(:backend mysql)
         :source-table "users"
         :columns '("id" "name" "note")
         :column-defs '((:name "id" :type-category numeric)
                        (:name "name" :type-category text)
                        (:name "note" :type-category text))
         :rows '((1 "alice" "before"))
         :row-identity (clutch-test--primary-row-identity "users" '("id") '(0))
         :render t)
      (let (record-buf edit-buf)
        (unwind-protect
            (progn
              (switch-to-buffer result-buf)
              (clutch--goto-cell 0 2)
              (call-interactively #'clutch-result-open-record)
              (setq record-buf (current-buffer))
              (goto-char (point-min))
              (search-forward "note")
              (beginning-of-line)
              (call-interactively #'clutch-result-edit-cell)
              (setq edit-buf (current-buffer))
              (erase-buffer)
              (insert "after")
              (call-interactively #'clutch-result-edit-finish)
              (should (eq (current-buffer) record-buf))
              (should (eq (get-text-property (point) 'clutch-col-idx) 2))
              (should (string-match-p "note\\s-*:\\s-*after" (buffer-string)))
              (with-current-buffer result-buf
                (should (equal clutch--pending-edits '((([1] . 2) . "after"))))))
          (when (buffer-live-p edit-buf) (kill-buffer edit-buf))
          (when (buffer-live-p record-buf) (kill-buffer record-buf)))))))

(ert-deftest clutch-test-record-edit-unchanged-numeric-does-not-stage ()
  "Submitting an unchanged numeric Record field should not stage an edit."
  (clutch-test--with-pop-to-buffer-capture edit-buf
    (clutch-test--with-result-state-buffer result-buf
        (:connection (make-clutch-test-conn :table "orders"
                                            :columns '((:name "qty" :type "int")))
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
          (with-current-buffer record-buf
            (clutch-result-edit-cell))
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

(provide 'clutch-test-ui)

;;; clutch-test-ui.el ends here
