# Troubleshooting Guide

## Common Issues and Solutions

### 1. Connection Errors
**Error**: `Can't connect to MySQL server`
**Solution**: 
- Check your `.env` file credentials
- Verify MySQL server is running
- Check firewall settings
- Test connection using mysql command line

### 2. Character Encoding Issues
**Error**: `Incorrect string value`
**Solution**:
Add charset to connection:
```python
'charset': 'utf8mb4',
'use_unicode': True