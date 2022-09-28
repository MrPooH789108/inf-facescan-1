"""This module provides the innoflex app CLI."""

from typing import Optional
import configparser
import typer

from infapp import __app_name__, __version__

app = typer.Typer()
config_path = "/opt/innoflex/config/configfile.ini"
config_obj = configparser.ConfigParser()
config_obj.read(config_path)
inflog = config_obj["log"]
logpath = inflog["path"]

def config_path_callback(value: bool):
    if value:
        print(f"config path: {config_path}")
        raise typer.Exit()


def edit_config(section, key, value):
    try:
        # Read config.ini file
        edit = configparser.ConfigParser()
        edit.read(config_path)
        # Get the postgresql section
        config_section = edit[section]
        # Update value
        config_section[key] = value
        # Write changes back to file
        with open(config_path, 'w') as configfile:
            edit.write(configfile)
        return True

    except Exception as e:
        return False

@app.command()
def conf(
    amqp_endpoint: str = typer.Option(
        None, "--amqp-endpoint", help="set AMQP endpoint to config file.", show_default=False),
    amqp_port: str = typer.Option(
        None, "--amqp-port", help="set AMQP port to config file.", show_default=False),
    amqp_virtualhost: str = typer.Option(
        None, "--amqp-virtualhost", help="set AMQP virtualhost to config file.", show_default=False),
    db_nodes: str = typer.Option(
        None, "--db-nodes", help="set database nodes to config file.", show_default=False),
    db_port: str = typer.Option(
        None, "--db-port", help="set database port to config file.", show_default=False),
    db_replicaset: str = typer.Option(
        None, "--db-replicaset", help="set database replicaset to config file.", show_default=False),
    mqtt_endpoint: str = typer.Option(
        None, "--mqtt-endpoint", help="set MQTT endpoint to config file.", show_default=False),
    mqtt_groupid: str = typer.Option(
        None, "--mqtt-groupid", help="set MQTT groupid to config file.", show_default=False),
    path: Optional[bool] = typer.Option(
        None, "--path", callback=config_path_callback, help="config file location."),
        
) -> None:
    """ config environment and variables """
    if amqp_endpoint != None:
        ae = edit_config("amqp", "endpoint", amqp_endpoint)
        if ae:
            typer.secho(
                f"amqp_endpoint = {amqp_endpoint} ", fg=typer.colors.GREEN,)

    if amqp_port != None:
        ap = edit_config("amqp", "port", amqp_port)
        if ap:
            typer.secho(f"amqp_port = {amqp_port} ", fg=typer.colors.GREEN,)

    if amqp_virtualhost != None:
        av = edit_config("amqp", "virtualhost", amqp_virtualhost)
        if av:
            typer.secho(
                f"amqp_virtualhost = {amqp_virtualhost} ", fg=typer.colors.GREEN,)

    if db_nodes != None:
        de = edit_config("db", "endpoint", db_nodes)
        if de:
            typer.secho(
                f"db_nodes = {db_nodes} ", fg=typer.colors.GREEN,)

    if db_port != None:
        dp = edit_config("db", "port", db_port)
        if dp:
            typer.secho(f"db_port = {db_port} ", fg=typer.colors.GREEN,)

    if db_replicaset != None:
        dr = edit_config("db", "replicaset", db_replicaset)
        if dr:
            typer.secho(
                f"db_replicaset = {db_replicaset} ", fg=typer.colors.GREEN,)

    if mqtt_endpoint != None:
        me = edit_config("mqtt", "endpoint", mqtt_endpoint)
        if me:
            typer.secho(
                f"mqtt_endpoint = {mqtt_endpoint} ", fg=typer.colors.GREEN,)

    if mqtt_groupid != None:
        mg = edit_config("mqtt", "groupid", mqtt_groupid)
        if mg:
            typer.secho(
                f"mqtt_groupid = {mqtt_groupid} ", fg=typer.colors.GREEN,)

def _version_callback(value: bool) -> None:
    if value:
        typer.echo(f"{__app_name__} v{__version__}")
        raise typer.Exit()


@app.callback()
def main(
    version: Optional[bool] = typer.Option(
        None,
        "--version",
        "-v",
        help="Show the application's version and exit.",
        callback=_version_callback,
        is_eager=True,
    )
) -> None:
    return
