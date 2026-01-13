from .main import main

class _DataFeedModule:
    def __call__(self, ORB_URL=None, ORB_PASSWORD=None, ORB_USERNAME=None, epochs=None, syms=None, month=None, year=None, symbol_type="SPOT"):
        if (ORB_PASSWORD == None) or (ORB_USERNAME == None) or (ORB_URL == None):
            return "[ERROR] ORB CREDS or URL not PROVIDED"
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
        return main(ORB_URL=ORB_URL, ORB_PASSWORD=ORB_PASSWORD, ORB_USERNAME=ORB_USERNAME, epochs=epochs, syms=syms, symbol_type=symbol_type, year=year, month=month)

# Replace this module with a callable instance
import sys
sys.modules[__name__] = _DataFeedModule()
