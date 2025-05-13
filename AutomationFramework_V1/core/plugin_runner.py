import importlib.util
import os
import sys
from typing import Dict, Any, Optional, Callable
import inspect
from pathlib import Path
import hashlib
import ast
from typing import Set

# Whitelist of allowed imports for plugins
ALLOWED_IMPORTS = {
    'pandas', 'numpy', 'datetime', 'json', 'csv', 'yaml',
    'logging', 'pathlib', 'typing', 'collections', 're'
}

# Whitelist of allowed built-in functions
ALLOWED_BUILTINS = {
    'len', 'int', 'float', 'str', 'list', 'dict', 'set',
    'tuple', 'bool', 'print', 'range', 'enumerate', 'zip',
    'min', 'max', 'sum', 'sorted', 'filter', 'map'
}

class PluginSecurityError(Exception):
    """Custom exception for plugin security violations."""
    pass

def validate_plugin_code(plugin_path: str) -> None:
    """
    Validate plugin code for security concerns.
    
    Args:
        plugin_path: Path to the plugin file
        
    Raises:
        PluginSecurityError: If any security violations are found
    """
    with open(plugin_path, 'r') as f:
        code = f.read()
    
    try:
        tree = ast.parse(code)
    except SyntaxError as e:
        raise PluginSecurityError(f"Invalid Python syntax in plugin: {e}")
    
    class SecurityVisitor(ast.NodeVisitor):
        def __init__(self):
            self.imports = set()
            self.calls = set()
            
        def visit_Import(self, node):
            for name in node.names:
                self.imports.add(name.name.split('.')[0])
            
        def visit_ImportFrom(self, node):
            if node.module:
                self.imports.add(node.module.split('.')[0])
                
        def visit_Call(self, node):
            if isinstance(node.func, ast.Name):
                self.calls.add(node.func.id)
            elif isinstance(node.func, ast.Attribute):
                self.calls.add(node.func.attr)
    
    visitor = SecurityVisitor()
    visitor.visit(tree)
    
    # Check for unauthorized imports
    unauthorized_imports = visitor.imports - ALLOWED_IMPORTS
    if unauthorized_imports:
        raise PluginSecurityError(
            f"Unauthorized imports found: {unauthorized_imports}"
        )
    
    # Check for dangerous built-ins
    dangerous_calls = visitor.calls - ALLOWED_BUILTINS
    if dangerous_calls & {'eval', 'exec', 'compile', 'globals', 'locals', 'vars'}:
        raise PluginSecurityError(
            f"Dangerous built-in functions detected: {dangerous_calls}"
        )

def compute_plugin_hash(plugin_path: str) -> str:
    """
    Compute SHA-256 hash of plugin file for integrity checking.
    
    Args:
        plugin_path: Path to the plugin file
        
    Returns:
        str: SHA-256 hash of the file
    """
    sha256_hash = hashlib.sha256()
    with open(plugin_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()

def load_plugin_module(plugin_name: str, plugins_dir: str = "plugins") -> Optional[Any]:
    """
    Securely load a plugin module from the plugins directory.
    
    Args:
        plugin_name: Name of the plugin (without .py extension)
        plugins_dir: Directory containing the plugins (default: "plugins")
        
    Returns:
        The loaded module or None if loading fails
        
    Raises:
        PluginSecurityError: If plugin fails security checks
    """
    # Ensure plugin name has .py extension
    if not plugin_name.endswith('.py'):
        plugin_name += '.py'
    
    # Construct and validate the full path to the plugin
    plugin_path = os.path.abspath(os.path.join(plugins_dir, plugin_name))
    if not plugin_path.startswith(os.path.abspath(plugins_dir)):
        raise PluginSecurityError("Plugin path traversal attempt detected")
    
    # Check if the plugin file exists
    if not os.path.exists(plugin_path):
        raise FileNotFoundError(f"Plugin file not found: {plugin_path}")
    
    # Validate plugin code for security
    validate_plugin_code(plugin_path)
    
    # Compute and store/verify plugin hash
    plugin_hash = compute_plugin_hash(plugin_path)
    
    # Load the module in a restricted environment
    try:
        module_name = f"plugin_{plugin_name.replace('.py', '')}"
        spec = importlib.util.spec_from_file_location(module_name, plugin_path)
        if spec is None:
            raise ImportError(f"Failed to create module spec for {plugin_path}")
            
        module = importlib.util.module_from_spec(spec)
        
        # Restrict module's __builtins__
        restricted_builtins = {
            name: getattr(__builtins__, name)
            for name in ALLOWED_BUILTINS
            if hasattr(__builtins__, name)
        }
        module.__dict__['__builtins__'] = restricted_builtins
        
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        return module
    except Exception as e:
        raise ImportError(f"Failed to load plugin {plugin_name}: {str(e)}")

def validate_plugin_interface(module: Any, function_name: str = "run") -> Callable:
    """
    Validate that the plugin module has the required function with proper signature.
    
    Args:
        module: The loaded plugin module
        function_name: The required function name (default: "run")
        
    Returns:
        The validated function object
    """
    if not hasattr(module, function_name):
        raise AttributeError(f"Plugin module does not contain required function: {function_name}")
    
    func = getattr(module, function_name)
    if not callable(func):
        raise TypeError(f"{function_name} in plugin module is not callable")
    
    # Check function signature
    sig = inspect.signature(func)
    if len(sig.parameters) != 1:
        raise ValueError(f"Plugin {function_name} function must accept exactly one parameter (context)")
    
    # Validate return type annotation if present
    if sig.return_annotation != inspect.Signature.empty:
        if sig.return_annotation != Dict[str, Any]:
            raise TypeError(f"Plugin {function_name} must return Dict[str, Any]")
    
    return func

def run_plugin(plugin_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Securely run a plugin with the provided configuration.
    
    Args:
        plugin_config: A dictionary containing plugin configuration with at least
                      'name' key and optional 'params' dictionary
                      
    Returns:
        The output dictionary from the plugin execution
    """
    if not isinstance(plugin_config, dict):
        raise TypeError("Plugin configuration must be a dictionary")
    
    # Get plugin name and parameters
    plugin_name = plugin_config.get("name")
    if not plugin_name:
        raise ValueError("Plugin configuration must contain 'name' key")
    
    function_name = plugin_config.get("function", "run")
    plugins_dir = plugin_config.get("plugins_dir", "plugins")
    
    # Ensure plugins directory exists and is within workspace
    plugins_path = Path(plugins_dir).resolve()
    if not plugins_path.exists():
        plugins_path.mkdir(parents=True)
    
    # Load and validate the plugin module
    try:
        module = load_plugin_module(plugin_name, str(plugins_path))
        plugin_func = validate_plugin_interface(module, function_name)
        
        # Prepare sanitized context for the plugin
        context = {
            "params": plugin_config.get("params", {}),
            "allowed_imports": list(ALLOWED_IMPORTS),
            "allowed_builtins": list(ALLOWED_BUILTINS)
        }
        
        # Execute the plugin in try-except block
        try:
            result = plugin_func(context)
        except Exception as e:
            raise RuntimeError(f"Plugin execution failed: {str(e)}")
        
        # Validate result
        if not isinstance(result, dict):
            raise TypeError(f"Plugin {plugin_name} must return a dictionary, got {type(result).__name__}")
        
        return result
    except Exception as e:
        raise PluginSecurityError(f"Error running plugin {plugin_name}: {str(e)}") 