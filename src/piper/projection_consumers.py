"""
We'll need to manage multiple ways to consume the FHIR resources once they are
projected. These will all support the same function and will be passed to the
streamer to allow flexible selections.
"""


# Write to a JSON file suitable for dewrangle
#
class DewrangleJSON:
    """Basically dump the resources to a single JSON file as members of a list.

    We'll buffer N resources before dumping the contents of the file.

    It should be noted that the
    """

    def __init__(self, filename, buffersize=100):
        self.filename = filename
        self.file = None
        self.buffersize = buffersize
        self.resources = []

    def __enter__(self):
        """We could create the file here, but let's wait until there is
        actually something to write to it before we do."""
        return self

    def _dump_buffer_to_file(self):
        """We'll cache a small number of resources. This is probably already
        done at the OS level...but it seems simple enough and we can adjust
        the size of the buffer."""
        start_with_comma = True
        if self.file is None:
            start_with_comma = False
            self.file = open(self.filename, "wt")
            self.file.write("[\n")
        if len(self.resources) > 0:
            if start_with_comma:
                self.file.write(",\n")
            self.file.write(",\n".join(self.resources))
        self.resources = []

    def __call__(self, resource):
        """Feed in the resources one at a time from our iteration"""
        self.resources.append(resource)

        if len(self.resources) >= self.buffersize:
            self._dump_buffer_to_file()

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Make sure that the file's list is properly closed before closing the
        the file entirely."""
        if self.file:
            self.file.write("\n]")
            self.file.close()
