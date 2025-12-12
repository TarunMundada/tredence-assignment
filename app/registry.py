TOOLS = {}

def register(name):
    def decorator(fn):
        TOOLS[name] = fn
        return fn
    return decorator

def get_tool(name):
    return TOOLS.get(name)

def list_tools():
    return list(TOOLS.keys())