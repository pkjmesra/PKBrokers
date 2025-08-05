import unittest
from unittest.mock import patch, MagicMock
from pkbrokers.kite.authenticator import KiteAuthenticator
import requests

class TestKiteAuthenticator(unittest.TestCase):
    
    def setUp(self):
        self.credentials = {
            "api_key": "test_api_key",
            "username": "test_user",
            "password": "test_password",
            "totp": "test_totp_secret"
        }
        self.authenticator = KiteAuthenticator(timeout=10)
        
    @patch('requests.Session.get')
    @patch('requests.Session.post')
    @patch('pyotp.TOTP.now')
    def test_successful_authentication(self, mock_totp, mock_post, mock_get):
        # Mock initial session request
        mock_get.return_value = MagicMock()
        
        # Mock login response
        mock_login_response = MagicMock()
        mock_login_response.json.return_value = {"data": {"request_id": "test123"}}
        mock_login_response.headers = {}
        
        # Mock TOTP response
        mock_totp_response = MagicMock()
        mock_totp_response.headers = {"Set-Cookie": "enctoken=test_enctoken; other=cookie"}
        
        mock_post.side_effect = [mock_login_response, mock_totp_response]
        mock_totp.return_value = "123456"
        
        enctoken = self.authenticator.get_enctoken(**self.credentials)
        self.assertEqual(enctoken, "test_enctoken")
        
    def test_missing_credentials(self):
        with self.assertRaises(ValueError):
            self.authenticator.get_enctoken(api_key="only_key")
            
    @patch('requests.Session.get')
    def test_network_error(self, mock_get):
        mock_get.side_effect = requests.exceptions.RequestException("Network error")
        with self.assertRaises(requests.exceptions.RequestException):
            self.authenticator.get_enctoken(**self.credentials)
            
    @patch('requests.Session.get')
    @patch('requests.Session.post')
    def test_invalid_response(self, mock_post, mock_get):
        mock_get.return_value = MagicMock()
        mock_post.return_value = MagicMock()
        mock_post.return_value.json.side_effect = ValueError("Invalid JSON")
        
        with self.assertRaises(ValueError):
            self.authenticator.get_enctoken(**self.credentials)
    
    def test_enctoken_extraction(self):
        test_cases = [
            ("enctoken=abc123", "abc123"),
            ("other=cookie; enctoken=def456", "def456"),
            ("enctoken=ghi,789; other=data", "ghi"),
            ("no_token_here", ""),
            (None, "")
        ]
        
        for cookies, expected in test_cases:
            result = self.authenticator._extract_enctoken(cookies)
            self.assertEqual(result, expected)

if __name__ == "__main__":
    unittest.main()