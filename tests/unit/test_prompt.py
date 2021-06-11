from unittest import TestCase

import mock

from dvc.prompt import confirm


class TestConfirm(TestCase):
    @mock.patch("sys.stdout.isatty", return_value=True)
    @mock.patch("dvc.prompt.input", side_effect=EOFError)
    def test_eof(self, mock_input, mock_isatty):
        ret = confirm("message")
        mock_isatty.assert_called()
        mock_input.assert_called()
        self.assertFalse(ret)
