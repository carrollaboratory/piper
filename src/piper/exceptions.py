class ProjectionError(Exception):
    """Malformed FHIR Resource from template file"""

    def __init__(self, decode_err, body, template_name="Unknown"):
        lnum = decode_err.lineno
        colno = decode_err.colno
        lines = body.splitlines()
        errline = lines[lnum - 1] if lnum < len(lines) else "Unknown"

        message = f"An error was encountered while rendering, '{template_name}'\nLine #: {lnum}\nCol Num #: {colno}\n----------\n\n{errline}"

        super().__init__(message)
        self.lnum = lnum
        self.colno = colno
        self.errline = errline
        self.template_name = template_name
