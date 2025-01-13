#!/usr/bin/env python3

import sys

import click

from cache.blog_cache import create_blog_cache
from cache.blog_category_cache import create_blog_category_cache
from cache.company_cache import create_company_cache
from cache.company_category_cache import create_company_category_cache
from cache.glossary_cache import create_glossary_cache
from cache.glossary_category_cache import create_glossary_category_cache
from cache.post_cache import create_post_cache
from cache.post_category_cache import create_post_category_cache


@click.group()
def cli():
    pass


@cli.command()
def build_all_caches():
    """
    Build all the caches
    """
    # create_blog_cache()
    # create_blog_category_cache()
    # create_glossary_cache()
    # create_glossary_category_cache()
    create_company_cache()
    create_company_category_cache()
    create_post_cache()
    create_post_category_cache()


@cli.command()
def build_blog_cache():
    """
    Build the blog cache
    """
    create_blog_cache()


@cli.command()
def build_blog_category_cache():
    """
    Build the blog_category cache
    """
    create_blog_category_cache()


@cli.command()
def build_company_cache():
    """
    Build the company cache
    """
    create_company_cache()


@cli.command()
def build_company_category_cache():
    """
    Build the company_category cache
    """
    create_company_category_cache()


@cli.command()
def build_glossary_cache():
    """
    Build the glossary cache
    """
    create_glossary_cache()


@cli.command()
def build_glossary_category_cache():
    """
    Build the glossary_category cache
    """
    create_glossary_category_cache()


@cli.command()
def build_post_cache():
    """
    Build the post cache
    """
    create_post_cache()


@cli.command()
def build_post_category_cache():
    """
    Build the post_category cache
    """
    create_post_category_cache()


def usage(command):
    with click.Context(command) as ctx:
        click.echo(command.get_help(ctx))


if __name__ == "__main__":
    cli()
    sys.exit(0)
