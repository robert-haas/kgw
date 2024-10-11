# Minimal Makefile for generating a HTML documentation website with Sphinx

SPHINXOPTS    =
SPHINXBUILD   = sphinx-build
SOURCEDIR     = source
BUILDDIR      = build

%:
	# Delete previous build
	@rm -rf build
	# Copy examples directory into docs source (required by nbsphinx to convert Jupyter notebooks into HTML)
	@-rm -rf source/code/examples
	@-mkdir -p source/code/
	@-rm -rf ../examples/a_user_chosen_directory ../examples/a_workdir_for_kgw
	@-cp -rf ../examples source/code/examples
	# Remove undesired items in examples directory
	@-rm -rf source/code/examples/kgw_workdir
	@-find source/code/examples -name ".ipynb_checkpoints" -type d -exec rm -rf {} \; 2> /dev/null || true
	# Copy README.rst into docs source and modify its header to become package_references.rst
	@-echo "Package References" > source/rst/package_references.rst
	@-echo "==================" >> source/rst/package_references.rst
	@-tail -n +3 ../README.rst >> source/rst/package_references.rst
	# Build the docs
	@$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)
	# Remove folders from build directory that are not needed for HTML representation
	@-rm -rf build/doctrees
	@-rm -rf build/html/_sources
	# Remove examples directory from docs source
	@-rm -rf source/code/examples
	
