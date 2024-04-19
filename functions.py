def allowed_file(filename):
    try:
        ALLOWED_EXTENSIONS = {'csv', 'pdf', 'xlsx', 'txt', 'doc', 'docx'}
        return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS
    except Exception as e:
        return f"Error checking allowed file: {e}"