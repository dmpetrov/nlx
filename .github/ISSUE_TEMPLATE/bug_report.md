---
name: "\U0001F41B Bug Report"
about: Create a bug report to help us improve DVC
---

# Bug Report

<!--
## Issue name

Issue names must follow the pattern `command: description` where the command is the dvc command that you are trying to run. The description should describe the consequence of the bug. 

Example: `repro: doesn't detect input changes`
-->

## Description

<!--
A clear and concise description of what the bug is.
-->

### Reproduce

<!--
Step list of how to reproduce the bug
-->

<!--
Example:

1. dvc init
2. Copy dataset.zip to the directory
3. dvc add dataset.zip
4. dvc run -d dataset.zip -o model ./train.sh
5. modify dataset.zip
6. dvc repro
-->

### Expected

<!--
A clear and concise description of what you expect to happen.
-->

### Environment information

<!--
This is required to ensure that we can reproduce the bug.
-->

**Output of `dvc doctor`:**

```console
$ dvc doctor
```

**Additional Information (if any):**

<!--
Please check https://github.com/iterative/dvc/wiki/Debugging-DVC on ways to gather more information regarding the issue.

If applicable, please also provide a `--verbose` output of the command, eg: `dvc add --verbose`.
If the issue is regarding the performance, please attach the profiling information and the benchmark comparisons.
-->
