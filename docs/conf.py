# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "Starburst Data Products Client"
copyright = "2025, Starburst Data"
author = "Starburst Data"
release = "0.1.0"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = ["sphinx.ext.autodoc", "sphinx.ext.napoleon", "sphinx.ext.autosummary", "sphinxcontrib.jquery", "sphinx_mdinclude"]

templates_path = ["_templates"]
exclude_patterns = ["_build", ".DS_Store"]


# -- Options for HTML output ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_favicon = "_static/img/favicon.ico"
html_theme = "sphinx_material"
html_theme_options = {
    # 'base_url': 'https://docs.starburst.io/' + get_context() + '/', # sitemap generation depends on this
    "globaltoc_depth": 4,
    "globaltoc_collapse": True,
    "globaltoc_includehidden": True,  # necessary so that sub pages like in Hive show up
    "theme_color": "000A2C",  # midnight blue
    "color_primary": "",  # set in starburst.css
    "color_accent": "",  # set in starburst.css
    "master_doc": False,
    "google_analytics_account": " UA-114610397-1",
    "nav_links": [
        # if order changes, set new active underline in starbust.css
        {
            "href": "usage",
            "internal": True,
            "title": "Get started",
        }
    ],
    "version_dropdown": False,
}

# too long for nav
html_title = project
html_permalinks = True
html_permalinks_icon = "#"
html_show_copyright = False
html_show_sphinx = False
html_copy_source = False
html_show_sourcelink = False
html_static_path = ["_static"]
html_logo = "_static/img/starburst_KO-T.png"
html_context = {"homepage_url": "http://www.starburst.io"}

html_sidebars = {"**": ["logo-text.html", "globaltoc.html", "localtoc.html", "searchbox.html"]}

html_css_files = [
    "starburst.css",
]

html_js_files = [
    "main.js",
]
