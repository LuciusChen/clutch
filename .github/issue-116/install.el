;;; install.el --- install clutch's MELPA dependencies  -*- lexical-binding: t -*-
(require 'package)
(setq package-archives '(("melpa" . "https://melpa.org/packages/")
                         ("gnu" . "https://elpa.gnu.org/packages/")))
(package-initialize)
(package-refresh-contents)
(dolist (pkg '(transient mysql))
  (unless (package-installed-p pkg) (package-install pkg)))
