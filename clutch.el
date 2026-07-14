;;; clutch.el --- Interactive database client -*- lexical-binding: t; -*-
;; Copyright (C) 2025-2026 Lucius Chen
;; SPDX-License-Identifier: GPL-3.0-or-later
;; Author: Lucius Chen <chenyh572@gmail.com>
;; Assisted-by: OpenAI Codex:gpt-5.5
;; Maintainer: Lucius Chen <chenyh572@gmail.com>
;; Version: 0.2.4
;; Package-Requires: ((emacs "29.1") (transient "0.3.7"))
;; Keywords: comm, data, tools
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

;; Interactive database client with native and JDBC backends.
;;
;; Provides:
;; - `clutch-mode': SQL editing major mode (derived from `sql-mode')
;; - `clutch-repl': REPL via `comint-mode'
;; - Query execution with horizontally scrollable result tables
;; - Object discovery and completion
;;
;; Entry points:
;;   M-x clutch-mode      — open a SQL editing buffer
;;   M-x clutch-repl      — open a REPL
;;   Open a .mysql file   — activates clutch-mode automatically

;;; Code:

(defgroup clutch nil
  "Interactive database lens."
  :group 'comm
  :prefix "clutch-")

(defcustom clutch-result-max-rows 500
  "Maximum number of rows to display in result tables."
  :type 'natnum
  :group 'clutch)

(require 'clutch-backend)
(require 'clutch-diagnostics)
(require 'clutch-connection)
(require 'clutch-query)
(require 'clutch-schema)
(require 'clutch-sql)
(require 'clutch-ui)
(require 'clutch-object)
(require 'clutch-edit)
(require 'clutch-result)

;;;; Customization

(defface clutch-field-name-face
  '((((class color) (background light))
     :weight bold :foreground "#2563eb")
    (((class color) (background dark))
     :weight bold :foreground "#b8d7ec")
    (t :weight bold))
  "Face for database field and column names."
  :group 'clutch)

(defface clutch-header-face
  '((t :weight bold))
  "Face for header text that is not a database field name."
  :group 'clutch)

(defface clutch-key-hint-key-face
  '((t :inherit transient-key))
  "Face for shortcut keys shown in Clutch header-line hints."
  :group 'clutch)

(defface clutch-key-hint-description-face
  '((t :inherit header-line))
  "Face for shortcut descriptions shown in Clutch header-line hints."
  :group 'clutch)

(defface clutch-insert-field-tag-face
  '((t :inherit shadow))
  "Face for metadata tags in the insert buffer."
  :group 'clutch)

(defface clutch-insert-field-error-face
  '((((class color) (background light))
     :underline (:color "#b91c1c" :style wave))
    (((class color) (background dark))
     :underline (:color "#fca5a5" :style wave))
    (t :inherit error))
  "Face for invalid values in the insert buffer."
  :group 'clutch)

(defface clutch-insert-inline-error-face
  '((t :inherit error))
  "Face for inline `insert-buffer' validation messages."
  :group 'clutch)

(defface clutch-insert-active-field-face
  '((t :inherit hl-line))
  "Face for the active line in the insert buffer."
  :group 'clutch)

(defface clutch-insert-active-field-name-face
  '((((class color) (background light))
     :weight bold :foreground "#2563eb")
    (((class color) (background dark))
     :weight bold :foreground "#b8d7ec")
    (t :weight bold))
  "Face for the active `insert-buffer' field prefix."
  :group 'clutch)

(defface clutch-header-active-face
  '((((class color) (background light))
     :background "#e5e7eb" :weight bold)
    (((class color) (background dark))
     :background "#263238" :weight bold)
    (t :weight bold))
  "Face for the column header under the cursor."
  :group 'clutch)

(defface clutch-border-face
  '((t :inherit shadow))
  "Face for table borders (pipes and separators)."
  :group 'clutch)

(defface clutch-object-source-face
  '((t :inherit shadow))
  "Face for object-source annotations in minibuffer completions."
  :group 'clutch)

(defface clutch-object-public-source-face
  '((t :inherit shadow))
  "Face for PUBLIC object-source annotations in minibuffer completions."
  :group 'clutch)

(defface clutch-object-type-face
  '((t :inherit shadow))
  "Face for object-type annotations in minibuffer completions."
  :group 'clutch)

(defface clutch-null-face
  '((t :inherit shadow :slant italic))
  "Face for NULL values."
  :group 'clutch)

(defface clutch-modified-face
  '((((class color) (background light))
     :inherit warning :background "#fff3cd")
    (((class color) (background dark))
     :inherit warning :background "#3d2b00")
    (t :inherit warning))
  "Face for staged-edit cell values."
  :group 'clutch)

(defface clutch-fk-face
  '((t :inherit font-lock-type-face :underline t))
  "Face for foreign key column values.
Underlined to indicate clickable (RET to follow)."
  :group 'clutch)

(defface clutch-marked-face
  '((t :inherit dired-marked))
  "Face for marked rows in result buffer."
  :group 'clutch)

(defface clutch-executed-sql-marker-face
  '((t :inherit success))
  "Face for the executed SQL gutter marker."
  :group 'clutch)

(defface clutch-failed-sql-marker-face
  '((t :inherit error))
  "Face for the failed SQL gutter marker."
  :group 'clutch)

(define-fringe-bitmap 'clutch-executed-sql-dot
  [24 60 126 255 255 126 60 24]
  nil nil 'center)

(defface clutch-pending-delete-face
  '((((class color) (background light))
     :background "#fde8e8" :foreground "#9b1c1c" :strike-through t)
    (((class color) (background dark))
     :background "#3b1212" :foreground "#fca5a5" :strike-through t)
    (t :strike-through t))
  "Face for rows staged for deletion."
  :group 'clutch)

(defface clutch-pending-insert-face
  '((((class color) (background light))
     :background "#e6f4ea" :foreground "#1e4620")
    (((class color) (background dark))
     :background "#1a3320" :foreground "#86efac")
    (t :inherit success))
  "Face for rows staged for insertion."
  :group 'clutch)

(defface clutch-error-summary-face
  '((((class color) (background dark)) :foreground "#ffb4b4" :weight semibold)
    (((class color) (background light)) :foreground "#b42318" :weight semibold)
    (t :inherit error :weight bold))
  "Face for SQL execution error summaries."
  :group 'clutch)

(provide 'clutch)
;;; clutch.el ends here
