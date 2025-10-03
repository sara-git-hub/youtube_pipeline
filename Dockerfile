FROM astrocrpublic.azurecr.io/runtime:3.0-10
# Installer les dépendances pour Soda Core et PostgreSQL
USER root
RUN pip3 install --no-cache-dir \
    setuptools \
    soda-core==3.5.5 \
    soda-core-postgres

USER astro