from pathlib import Path
import shutil
import sys

def copy_dir(src: Path, dst: Path):
	"""Recursively copy contents of src directory into dst.

	- Creates dst if it doesn't exist.
	- Overwrites files in dst with files from src.
	"""
	if not src.exists():
		print(f"Fuente no encontrada: {src}")
		sys.exit(1)

	dst.mkdir(parents=True, exist_ok=True)

	for item in src.iterdir():
		dest_item = dst / item.name
		if item.is_dir():
			copy_dir(item, dest_item)
		else:
			# copy2 to preserve metadata where possible
			shutil.copy2(item, dest_item)


def main():
	base = Path(__file__).resolve()
	# carpeta origen: project/output/reports
	src = base.parents[1] / "output" / "reports"
	# carpeta destino: <workspace>/site/content/reportes
	dst = base.parents[2] / "site" / "content" / "reportes"

	print(f"Copiando carpeta: {src} -> {dst}")
	copy_dir(src, dst)
	print("Copiado:", dst)


if __name__ == '__main__':
	main()
