# -*- coding: utf-8 -*-
"""Script for migrating data from various sources into the database."""
from argparse import ArgumentParser
from configparser import ConfigParser
from functools import partial
from typedb.api.connection.credential import TypeDBCredential
from typedb.client import SessionType, TypeDB
from loader.coronavirus.coronavirus_loader import load_coronavirus
from loader.dgidb.dgidb_loader import load_dgibd
from loader.disgenet.disgenet_loader import load_disgenet
from loader.hpa.hpa_loader import load_hpa
from loader.reactome.reactome_loader import load_reactome
from loader.semmed.pipeline import load_semmed
from loader.tissuenet.tissuenet_loader import load_tissuenet
from loader.uniprot.uniprot_loader import load_uniprot
from schema.initialise import initialise_database


def parse_config_args(filepath):
    config = ConfigParser(allow_no_value=True)
    config.read(filepath)
    args = dict()

    for section in config.sections():
        args[section] = dict()

        for option in config.options(section):
            value = config.get(section, option)

            if value == "":
                value = None

            args[section][option] = value

    return args


def command_parser():
    """Parse the command line arguments."""
    parser = ArgumentParser(
        description="Defines typedb-bio database and inserts data by calling separate loader scripts.",
    )

    parser.add_argument(
        "-c",
        "--config_path",
        type=str,
        default="config.ini",
        help="Path for config file. Command line arguments override file arguments. (default: \"config.ini\")",
    )

    parser.add_argument(
        "-d",
        "--database",
        type=str,
        help="Database name.",
    )

    parser.add_argument(
        "-a",
        "--address",
        type=str,
        help="Server address and port.",
    )

    parser.add_argument(
        "-s",
        "--server_type",
        type=str,
        choices=["CORE", "CLUSTER"],
        help="TypeDB server product in use, \"CORE\" or \"CLUSTER\".",
    )

    parser.add_argument(
        "-u",
        "--username",
        type=str,
        help="Username for TypeDB Cluster.",
    )

    parser.add_argument(
        "-p",
        "--password",
        type=str,
        help="Password for TypeDB Cluster. Cannot be supplied via config file.",
    )

    parser.add_argument(
        "-t",
        "--tls_cert_path",
        type=str,
        help="TLS certificate path for TypeDB Cluster.",
    )

    parser.add_argument(
        "-o",
        "--overwrite",
        type=bool,
        help="Overwrite existing database with the same name.",
    )

    parser.add_argument(
        "-b",
        "--commit_batch",
        type=int,
        help="Number of insert queries per commit.",
    )

    parser.add_argument(
        "-n",
        "--num_jobs",
        type=int,
        help="Maximum number of concurrently running jobs.",
    )

    return parser


def parse_args():
    command_args = vars(command_parser().parse_args())
    config_args = parse_config_args(command_args.pop("config_path"))

    bool_args = [
        "overwrite",
    ]

    int_args = [
        "max_proteins",
        "max_viruses",
        "max_pathways",
        "max_diseases",
        "max_drugs",
        "max_drug_interactions",
        "max_tissues",
        "max_publications",
        "max_protein_interactions",
        "commit_batch",
        "num_jobs",
    ]

    args = dict()

    for section in config_args:
        for arg in config_args[section]:
            if config_args[section][arg] is None:
                args[arg] = None
            elif arg in bool_args:
                args[arg] = bool(config_args[section][arg])
            elif arg in int_args:
                args[arg] = int(config_args[section][arg])
            else:
                args[arg] = config_args[section][arg]

    for arg in command_args:
        if arg == "password" or command_args[arg] is not None:
            args[arg] = command_args[arg]

    return args


if __name__ == "__main__":
    args = parse_args()

    if args["server_type"].lower() == "core":
        client_partial = partial(TypeDB.core_client, address=args["address"])
    elif args["server_type"].lower() == "cluster":
        credential = TypeDBCredential(username=args["username"], password=args["password"], tls_root_ca_path=args["tls_cert_path"])
        client_partial = partial(TypeDB.cluster_client, addresses=args["address"], credential=credential)
    else:
        raise ValueError("Unknown server type. Must be \"CORE\" or \"CLUSTER\".")

    print("Welcome to the TypeDB Bio database loader!")
    print("--------------------------------------------------")

    with client_partial() as client:
        initialise_database(client, args["database"], args["overwrite"])

        with client.session(args["database"], SessionType.DATA) as session:
            load_uniprot(session, args["max_proteins"], args["num_jobs"], args["commit_batch"])
            load_coronavirus(session, args["max_viruses"], args["num_jobs"], args["commit_batch"])
            load_reactome(session, args["max_pathways"], args["num_jobs"], args["commit_batch"])
            load_disgenet(session, args["max_diseases"], args["num_jobs"], args["commit_batch"])
            load_dgibd(session, args["max_drugs"], args["max_drug_interactions"], args["num_jobs"], args["commit_batch"])
            load_hpa(session, args["max_tissues"], args["num_jobs"], args["commit_batch"])
            load_semmed(session, args["max_publications"], args["num_jobs"], args["commit_batch"])
            load_tissuenet(session, args["max_protein_interactions"], args["num_jobs"], args["commit_batch"])

    print("All data loaded.")
    print("Goodbye!")
