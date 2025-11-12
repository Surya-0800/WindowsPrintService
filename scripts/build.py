#!/usr/bin/env python3
"""
Build script to create standalone executable using PyInstaller
Fixed version with better error handling and simplified dependencies
"""

import os
import sys
import subprocess
import shutil
from pathlib import Path

# Get project root directory
project_root = Path(__file__).parent.parent.absolute()
os.chdir(project_root)

def build_executable():
    """Build standalone executable with simplified approach"""
    print("🔨 Building Windows Print Service Executable")
    print("=" * 50)
    print(f"Project root: {project_root}")
    
    try:
        # Check if PyInstaller is installed
        try:
            import PyInstaller
            print("✓ PyInstaller found")
        except ImportError:
            print("Installing PyInstaller...")
            subprocess.run([sys.executable, "-m", "pip", "install", "pyinstaller"], check=True)
            print("✓ PyInstaller installed")
        
        # Clean previous builds
        build_dirs = ['build', 'dist', '__pycache__', 'scripts/__pycache__', 'src/__pycache__']
        for dir_name in build_dirs:
            if os.path.exists(dir_name):
                shutil.rmtree(dir_name)
                print(f"✓ Cleaned {dir_name}")
        
        # Simplified PyInstaller command
        cmd = [
            sys.executable, "-m", "PyInstaller",
            "--onefile",                       # Single executable
            "--console",                       # Keep console window
            "--name=WindowsPrintService",      # Executable name
            "--paths=src",                     # Add src to Python path
            "--hidden-import=win32print",      # Essential Windows modules
            "--hidden-import=win32api",
            "--hidden-import=uvicorn.main",    # Uvicorn main module
            "--hidden-import=fastapi",
            "--exclude-module=tkinter",        # Exclude unnecessary modules
            "--exclude-module=matplotlib",
            "--exclude-module=PIL",
            "--exclude-module=numpy",
            "main.py"                          # Main script
        ]
        
        print("Building executable...")
        print(f"Command: {' '.join(cmd)}")
        print("\nThis may take several minutes...")
        
        # Run with real-time output and timeout
        process = subprocess.Popen(
            cmd, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.STDOUT,
            universal_newlines=True,
            bufsize=1
        )
        
        # Print output in real-time
        while True:
            output = process.stdout.readline()
            if output == '' and process.poll() is not None:
                break
            if output:
                print(output.strip())
        
        rc = process.poll()
        
        if rc == 0:
            exe_path = Path("dist/WindowsPrintService.exe")
            if exe_path.exists():
                size_mb = exe_path.stat().st_size / (1024 * 1024)
                print(f"\n✓ Build successful!")
                print(f"✓ Executable: {exe_path}")
                print(f"✓ Size: {size_mb:.1f} MB")
                
                # Test the executable quickly
                test_exe(exe_path)
                
                # Create deployment package
                create_deployment_package()
                
                return True
            else:
                print("\n❌ Executable not found after build")
                return False
        else:
            print(f"\n❌ Build failed with return code {rc}")
            return False
            
    except KeyboardInterrupt:
        print("\n❌ Build cancelled by user")
        return False
    except Exception as e:
        print(f"\n❌ Build error: {e}")
        return False

def test_exe(exe_path):
    """Quick test of the built executable"""
    try:
        print(f"\n🧪 Testing executable...")
        
        # Test with --help flag (should be quick)
        result = subprocess.run(
            [str(exe_path), "--help"], 
            capture_output=True, 
            text=True, 
            timeout=10
        )
        
        if result.returncode == 0:
            print("✓ Executable test passed")
        else:
            print(f"⚠️  Executable test returned code {result.returncode}")
            if result.stderr:
                print(f"   Error: {result.stderr[:200]}...")
                
    except subprocess.TimeoutExpired:
        print("⚠️  Executable test timeout (may still work)")
    except Exception as e:
        print(f"⚠️  Executable test failed: {e}")

def create_deployment_package():
    """Create deployment package with all necessary files"""
    try:
        print("\n📦 Creating deployment package...")
        
        # Create deployment directory
        deploy_dir = Path("deployment")
        if deploy_dir.exists():
            shutil.rmtree(deploy_dir)
        deploy_dir.mkdir()
        
        # Copy executable
        shutil.copy("dist/WindowsPrintService.exe", deploy_dir)
        print("✓ Copied executable")
        
        # Copy existing batch files from scripts directory
        scripts_dir = Path("scripts")
        batch_files = [
            "install.bat", "uninstall.bat", "status.bat", 
            "start.bat", "stop.bat"
        ]
        
        for batch_file in batch_files:
            if (scripts_dir / batch_file).exists():
                shutil.copy(scripts_dir / batch_file, deploy_dir)
                print(f"✓ Copied {batch_file}")
            else:
                create_missing_batch_file(deploy_dir, batch_file)
        
        # Create console.bat for debugging
        create_console_bat(deploy_dir)
        
        # Copy documentation
        copy_documentation(deploy_dir)
        
        # Create ZIP package
        shutil.make_archive("WindowsPrintService-Deployment", 'zip', deploy_dir)
        
        print("✓ Deployment package created: WindowsPrintService-Deployment.zip")
        print("\nDeployment contents:")
        for item in sorted(deploy_dir.iterdir()):
            print(f"  - {item.name}")
        
    except Exception as e:
        print(f"❌ Deployment package creation failed: {e}")

def create_missing_batch_file(deploy_dir, filename):
    """Create missing batch files with basic content"""
    batch_content = {
        "install.bat": """@echo off
echo Installing Windows Print Service...
if "%1"=="" (
    echo Usage: install.bat ^<SERVER_URL^>
    echo Example: install.bat http://192.168.1.100:8000
    pause
    exit /b 1
)
WindowsPrintService.exe --install %1
pause
""",
        "uninstall.bat": """@echo off
echo Uninstalling Windows Print Service...
WindowsPrintService.exe --uninstall
pause
""",
        "status.bat": """@echo off
echo Checking Windows Print Service status...
WindowsPrintService.exe --status
pause
""",
        "start.bat": """@echo off
echo Starting Windows Print Service...
WindowsPrintService.exe --start
pause
""",
        "stop.bat": """@echo off
echo Stopping Windows Print Service...
WindowsPrintService.exe --stop
pause
"""
    }
    
    if filename in batch_content:
        with open(deploy_dir / filename, 'w') as f:
            f.write(batch_content[filename])
        print(f"✓ Created {filename}")

def create_console_bat(deploy_dir):
    """Create console.bat for debugging"""
    console_bat = """@echo off
echo =========================================
echo Windows Print Service - Console Mode
echo =========================================
echo Running in console mode for debugging...
echo Press Ctrl+C to stop
echo.

WindowsPrintService.exe --console
pause
"""
    with open(deploy_dir / "console.bat", 'w') as f:
        f.write(console_bat)
    print("✓ Created console.bat")

def copy_documentation(deploy_dir):
    """Copy documentation files"""
    doc_files = ["README.md", "requirements.txt"]
    
    for doc_file in doc_files:
        if Path(doc_file).exists():
            shutil.copy(doc_file, deploy_dir)
            print(f"✓ Copied {doc_file}")

if __name__ == "__main__":
    print("🚀 Windows Print Service Build Script")
    print("Press Ctrl+C to cancel at any time")
    print()
    
    success = build_executable()
    
    if success:
        print("\n🎉 Build completed successfully!")
        print("\nNext steps:")
        print("1. Test the executable: cd deployment && WindowsPrintService.exe --console")
        print("2. Deploy to target machines: Copy WindowsPrintService-Deployment.zip")
        print("3. Install on target: Extract zip and run install.bat as Administrator")
        print("\nTroubleshooting:")
        print("- If exe doesn't start, try console.bat to see errors")
        print("- Check that all dependencies are installed")
        print("- Verify Python modules are in src/ directory")
    else:
        print("\n❌ Build failed!")
        print("\nTroubleshooting steps:")
        print("1. Check that all Python files are in src/ directory")
        print("2. Verify requirements.txt has all dependencies")
        print("3. Try: pip install -r requirements.txt")
        print("4. Run: python main.py --console (test before building)")
        print("5. Check for import errors in your code")
        sys.exit(1)