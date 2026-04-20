import pytest
from unittest.mock import patch
from input_handler import InputHandler


@pytest.fixture
def handler():
    return InputHandler()


@pytest.fixture
def registered_handler():
    h = InputHandler()
    h.register_input(
        'age',
        {'type': int, 'required': True, 'min': 0, 'max': 120},
        {'type': 'Age must be an integer', 'required': 'Age is required',
         'min': 'Age cannot be negative', 'max': 'Age cannot exceed 120'},
    )
    h.register_input(
        'username',
        {'type': str, 'required': True, 'pattern': r'^[a-z_]+$', 'lowercase': True},
        {'pattern': 'Username must be lowercase letters and underscores only'},
    )
    return h


class TestRegisterInput:
    def test_register_creates_state_entry(self, handler):
        handler.register_input('field', {'type': str})
        state = handler.get_input_state('field')
        assert state == {'value': None, 'is_valid': False, 'errors': []}

    def test_register_default_empty_messages(self, handler):
        handler.register_input('field', {'type': str})
        # No error before validation — default messages dict is empty
        assert handler.get_input_errors('field') == []


class TestValidateInput:
    @patch('input_handler.log_error')
    def test_valid_int_passes(self, mock_log, registered_handler):
        assert registered_handler.validate_input('age', 25) is True
        mock_log.assert_not_called()

    @patch('input_handler.log_error')
    def test_type_mismatch_fails(self, mock_log, registered_handler):
        assert registered_handler.validate_input('age', 'twenty') is False
        errors = registered_handler.get_input_errors('age')
        assert any('Age must be an integer' in e for e in errors)
        mock_log.assert_called_once()

    @patch('input_handler.log_error')
    def test_required_empty_string_fails(self, mock_log, handler):
        handler.register_input('name', {'type': str, 'required': True})
        assert handler.validate_input('name', '') is False
        errors = handler.get_input_errors('name')
        assert errors  # at least one error

    @patch('input_handler.log_error')
    def test_required_none_fails(self, mock_log, handler):
        handler.register_input('name', {'type': str, 'required': True})
        assert handler.validate_input('name', None) is False

    @patch('input_handler.log_error')
    def test_min_violation_fails(self, mock_log, registered_handler):
        assert registered_handler.validate_input('age', -1) is False
        errors = registered_handler.get_input_errors('age')
        assert any('negative' in e for e in errors)

    @patch('input_handler.log_error')
    def test_max_violation_fails(self, mock_log, registered_handler):
        assert registered_handler.validate_input('age', 200) is False
        errors = registered_handler.get_input_errors('age')
        assert any('120' in e for e in errors)

    @patch('input_handler.log_error')
    def test_pattern_match_passes(self, mock_log, registered_handler):
        assert registered_handler.validate_input('username', 'alice') is True

    @patch('input_handler.log_error')
    def test_pattern_mismatch_fails(self, mock_log, registered_handler):
        assert registered_handler.validate_input('username', 'Alice123') is False
        errors = registered_handler.get_input_errors('username')
        assert any('lowercase' in e for e in errors)

    @patch('input_handler.log_error')
    def test_custom_validator_string_error(self, mock_log, handler):
        handler.register_input('score', {
            'type': int,
            'custom': lambda v: 'Must be even' if v % 2 != 0 else None,
        })
        assert handler.validate_input('score', 3) is False
        assert 'Must be even' in handler.get_input_errors('score')

    @patch('input_handler.log_error')
    def test_custom_validator_passes_on_none_return(self, mock_log, handler):
        handler.register_input('score', {
            'type': int,
            'custom': lambda v: None,
        })
        assert handler.validate_input('score', 4) is True

    def test_unregistered_input_raises(self, handler):
        with pytest.raises(ValueError, match="not registered"):
            handler.validate_input('ghost', 42)


class TestGetInputErrors:
    @patch('input_handler.log_error')
    def test_returns_empty_before_validation(self, mock_log, registered_handler):
        assert registered_handler.get_input_errors('age') == []

    @patch('input_handler.log_error')
    def test_returns_errors_after_failed_validation(self, mock_log, registered_handler):
        registered_handler.validate_input('age', -5)
        assert registered_handler.get_input_errors('age')

    @patch('input_handler.log_error')
    def test_errors_cleared_on_valid_input(self, mock_log, registered_handler):
        registered_handler.validate_input('age', -5)
        registered_handler.validate_input('age', 30)
        assert registered_handler.get_input_errors('age') == []


class TestSanitizeInput:
    def test_trims_whitespace_by_default(self, handler):
        handler.register_input('name', {'type': str})
        assert handler.sanitize_input('name', '  alice  ') == 'alice'

    def test_lowercase(self, handler):
        handler.register_input('tag', {'type': str, 'lowercase': True})
        assert handler.sanitize_input('tag', 'HELLO') == 'hello'

    def test_uppercase(self, handler):
        handler.register_input('code', {'type': str, 'uppercase': True})
        assert handler.sanitize_input('code', 'abc') == 'ABC'

    def test_round_numeric(self, handler):
        handler.register_input('price', {'type': float, 'round': 2})
        assert handler.sanitize_input('price', 3.14159) == 3.14

    def test_unregistered_raises(self, handler):
        with pytest.raises(ValueError, match="not registered"):
            handler.sanitize_input('ghost', 'x')


class TestResetInput:
    @patch('input_handler.log_error')
    def test_reset_clears_errors_and_value(self, mock_log, registered_handler):
        registered_handler.validate_input('age', -1)
        registered_handler.reset_input('age')
        state = registered_handler.get_input_state('age')
        assert state == {'value': None, 'is_valid': False, 'errors': []}

    def test_reset_unknown_key_is_noop(self, handler):
        handler.reset_input('nonexistent')  # should not raise
