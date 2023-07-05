def tissue_mapper(tissue):
    mapper = {
        "appendix": {
            "tissue-name": "appendix",
            "cell-name": None,
        },
        "adipocytes": {
            "tissue-name": "adipose tissue",
            "cell-name": "adipocytes",
        },
        "bone marrow_hematopoietic cells": {
            "tissue-name": "bone marrow",
            "cell-name": "hematopoietic cells",
        },
        "brain - cerebellum": {
            "tissue-name": "cerebellum",
            "cell-name": None,
        },
        "brain - cerebral coretx": {
            "tissue-name": "cerebral cortex",
            "cell-name": None,
        },
        "brain - hippocampus": {
            "tissue-name": "hippocampus",
            "cell-name": None,
        },
        "brain - lateral ventricle": {
            "tissue-name": None,
            "cell-name": None,
        },
        "breast": {
            "tissue-name": "breast",
            "cell-name": None,
        },
        "bronchus_respiratory epithelial cells": {
            "tissue-name": "bronchus",
            "cell-name": "respiratory epithelial cells",
        },
        "cervix": {
            "tissue-name": "cervix",
            "cell-name": None,
        },
        "chondrocytes": {
            "tissue-name": None,
            "cell-name": "chondrocytes",
        },
        "colon": {
            "tissue-name": "colon",
            "cell-name": None,
        },
        "duodenum_glandular cells": {
            "tissue-name": "duodenum",
            "cell-name": "glandular cells",
        },
        "endometrium": {
            "tissue-name": "endometrium",
            "cell-name": None,
        },
        "epididymis_glandular cells": {
            "tissue-name": "epididymis",
            "cell-name": "glandular cells",
        },
        "esophagus_squamous epithelial cells": {
            "tissue-name": "esophagus",
            "cell-name": "squamous epithelial cells",
        },
        "fallopian tube_glandular cells": {
            "tissue-name": "fallopian tube",
            "cell-name": "glandular cells",
        },
        "fibroblasts": {
            "tissue-name": None,
            "cell-name": "fibroblasts",
        },
        "gallbladder_glandular cells": {
            "tissue-name": "gallbladder",
            "cell-name": "glandular cells",
        },
        "glandular cells": {
            "tissue-name": None,
            "cell-name": "glandular cells",
        },
        "heart muscle_myocytes": {
            "tissue-name": "heart muscle",
            "cell-name": "cardiomyocytes",
        },
        "kidney": {
            "tissue-name": "kidney",
            "cell-name": None,
        },
        "liver": {
            "tissue-name": "liver",
            "cell-name": None,
        },
        "lung": {
            "tissue-name": "lung",
            "cell-name": None,
        },
        "lymph node": {
            "tissue-name": "lymph node",
            "cell-name": None,
        },
        "nasopharynx_respiratory epithelial cells": {
            "tissue-name": "nasopharynx",
            "cell-name": "respiratory epithelial cells",
        },
        "oral mucosa_squamous epithelial cells": {
            "tissue-name": "oral mucosa",
            "cell-name": "squamous epithelial cells",
        },
        "ovary": {
            "tissue-name": "ovary",
            "cell-name": None,
        },
        "pancreas": {
            "tissue-name": "pancreas",
            "cell-name": None,
        },
        "parathyroid gland": {
            "tissue-name": "parathyroid gland",
            "cell-name": None,
        },
        "peripheral nerve": {
            "tissue-name": None,
            "cell-name": "peripheral nerve",
        },
        "placenta": {
            "tissue-name": "placenta",
            "cell-name": None,
        },
        "prostate_glandular cells": {
            "tissue-name": "prostate",
            "cell-name": "glandular cells",
        },
        "rectum_glandular cells": {
            "tissue-name": "rectum",
            "cell-name": "glandular cells",
        },
        "salivary gland_glandular cells": {
            "tissue-name": "salivary gland",
            "cell-name": "glandular cells",
        },
        "seminal vesicle_glandular cells": {
            "tissue-name": "seminal vesicle",
            "cell-name": "glandular cells",
        },
        "skeletal muscle_myocytes": {
            "tissue-name": "skeletal muscle",
            "cell-name": "myocytes",
        },
        "skin": {
            "tissue-name": "skin",
            "cell-name": None,
        },
        "small intestine_glandular cells": {
            "tissue-name": "small intestine",
            "cell-name": "glandular cells",
        },
        "smooth muscle_smooth muscle cells": {
            "tissue-name": "smooth muscle",
            "cell-name": "smooth muscle cells",
        },
        "spleen": {
            "tissue-name": "spleen",
            "cell-name": None,
        },
        "stomach": {
            "tissue-name": "stomach",
            "cell-name": None,
        },
        "testis": {
            "tissue-name": "testis",
            "cell-name": None,
        },
        "thyroid gland": {
            "tissue-name": "thyroid gland",
            "cell-name": None,
        },
        "tonsil": {
            "tissue-name": "tonsil",
            "cell-name": None,
        },
        "urinary bladder_urothelial cells": {
            "tissue-name": "urinary bladder",
            "cell-name": "urothelial cells",
        },
        "vagina_squamous epithelial cells": {
            "tissue-name": "vagina",
            "cell-name": "squamous epithelial cells",
        },
    }

    try:
        return mapper[tissue]
    except KeyError:
        raise KeyError("Unrecognised tissue type: \"{}\". Add to TissueNet mapper.".format(tissue))
