from .main import main

class _DataFeedModule:
    def __call__(self, mongo_uri=None, syms=None, epochs=None):
        if mongo_uri == None:
            return "[ERROR] MONGO URI not provided"
        if syms == None:
            return ("[ERROR] SYMBOLS not provided")
        else:
            if type(syms) != list:
                return ("[ERROR] SYMBOLS must be a list")
        if epochs == None:
            return ("[ERROR] EPOCHS not provided")
        else:
            if type(epochs) != list:
                    return ("[ERROR] Epochs must be a list")
            if len(epochs) > 2:    
                return ("[ERROR] Epochs cannot exceed length of 2 elements")
        return main(
            mongo_uri=mongo_uri,
            syms=syms,
            epochs=epochs
        )

# Replace this module with a callable instance
import sys
sys.modules[__name__] = _DataFeedModule()
