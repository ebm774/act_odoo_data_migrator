# models/password_mixin.py
from odoo import models, fields, api
import base64
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
import logging

_logger = logging.getLogger(__name__)


class PasswordMixin(models.AbstractModel):
    """Simple mixin for encrypted password storage"""
    _name = 'password.mixin'
    _description = 'Password Encryption Mixin'

    @api.model
    def _get_encryption_key(self):
        """Generate a simple encryption key"""
        # Simple key generation - same for all instances
        secret = "odoo_password_encryption_key_v1"
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=b'simple_salt_12345',
            iterations=100000,
        )
        key = base64.urlsafe_b64encode(kdf.derive(secret.encode()))
        return key

    def encrypt_password(self, password):
        """Encrypt a password"""
        if not password:
            return False

        # Handle case where password might be boolean (the original error)
        if isinstance(password, bool):
            _logger.warning(f"Password field received boolean value: {password}")
            return False

        # Ensure password is string
        if not isinstance(password, str):
            password = str(password)

        try:
            fernet = Fernet(self._get_encryption_key())
            encrypted_bytes = fernet.encrypt(password.encode('utf-8'))
            # Convert to base64 for proper storage in Binary field
            return base64.b64encode(encrypted_bytes)
        except Exception as e:
            _logger.error(f"Password encryption failed: {e}")
            return False

    def decrypt_password(self, encrypted_password):
        """Decrypt a password"""
        if not encrypted_password:
            return None

        try:
            fernet = Fernet(self._get_encryption_key())
            # Decode from base64 first
            encrypted_bytes = base64.b64decode(encrypted_password)
            decrypted_bytes = fernet.decrypt(encrypted_bytes)
            return decrypted_bytes.decode('utf-8')
        except Exception as e:
            _logger.error(f"Password decryption failed: {e}")
            return None