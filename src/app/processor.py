
def process(extractor, transformer, loader):
    """
    Extract-Transform-Load process
    
    :param extractor: partial extractor function
    :param transformer: partial transformer function
    :param loader:  partial loader function
    """

    submissions_generator = extractor()

    events_generator = transformer(submissions_generator)

    loader(events_generator)
