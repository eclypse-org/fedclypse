.. toctree::
   :maxdepth: 2
   :hidden:

   Getting Started <self>
   Tutorials <source/tutorials/index.rst>
   FedRay Core API <source/core-api/index.rst>

.. image:: _static/images/fedray_logo_name_color.png

===============================================

Welcome to the `FedRay <https://github.com/vdecaro/fedray>`_ documentation! 


Why FedRay?
===========
FedRay is **a framework for Research in Federated 
Learning based on Ray**. It allows to easily *implement your FL algorithms* or *use 
off-the-shelf algorithms*, and distribute them seamlessly on Ray Clusters.

FedRay is a research project, and it is still under development.


.. _getting-started:

Getting Started
===============
To get started, you can easily install FedRay by running the following commands:

.. code-block:: bash

   pip install git+https://github.com/vdecaro/fedray

To ensure that the installation was successful, you can execute this simple python 
script:

.. code-block:: python

   import fedray
   print(fedray.__version__)
