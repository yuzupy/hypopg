/*-------------------------------------------------------------------------
 *
 * hypopg.c: Implementation of hypothetical indexes for PostgreSQL
 *
 * Some functions are imported from PostgreSQL source code, theses are present
 * in hypopg_import.* files.
 *
 * This program is open source, licensed under the PostgreSQL license.
 * For license terms, see the LICENSE file.
 *
 * Copyright (C) 2015-2018: Julien Rouhaud
 *
 *-------------------------------------------------------------------------
 */


#include "postgres.h"
#include "fmgr.h"

#include "include/hypopg.h"
#include "include/hypopg_import.h"
#include "include/hypopg_index.h"
#include "include/hypopg_table.h"

PG_MODULE_MAGIC;

/*--- Macros ---*/
#define HYPO_ENABLED() (isExplain && hypo_is_enabled)

/*--- Variables exported ---*/

bool isExplain;
bool hypo_is_enabled;
MemoryContext HypoMemoryContext;

/*--- Functions --- */

void		_PG_init(void);
void		_PG_fini(void);

Datum		hypopg_reset(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(hypopg_reset);

static void
hypo_utility_hook(
#if PG_VERSION_NUM >= 100000
				  PlannedStmt *pstmt,
#else
				  Node *parsetree,
#endif
				  const char *queryString,
#if PG_VERSION_NUM >= 90300
				  ProcessUtilityContext context,
#endif
				  ParamListInfo params,
#if PG_VERSION_NUM >= 100000
				  QueryEnvironment *queryEnv,
#endif
#if PG_VERSION_NUM < 90300
				  bool isTopLevel,
#endif
				  DestReceiver *dest,
				  char *completionTag);
static ProcessUtility_hook_type prev_utility_hook = NULL;

static void hypo_executorEnd_hook(QueryDesc *queryDesc);
static ExecutorEnd_hook_type prev_ExecutorEnd_hook = NULL;


static void hypo_get_relation_info_hook(PlannerInfo *root,
							Oid relationObjectId,
							bool inhparent,
							RelOptInfo *rel);
static get_relation_info_hook_type prev_get_relation_info_hook = NULL;

static void hypo_set_rel_pathlist_hook(PlannerInfo *root,
									   RelOptInfo *rel,
									   Index rti,
									   RangeTblEntry *rte);
static set_rel_pathlist_hook_type prev_set_rel_pathlist_hook = NULL;

static PartitionDesc hypo_RelationGetPartitionDesc_hook(Oid relid);
static RelationGetPartitionDesc_hook_type prev_RelationGetPartitionDesc_hook = NULL;

static PartitionKey hypo_RelationGetPartitionKey_hook(Oid relid);
static RelationGetPartitionKey_hook_type prev_RelationGetPartitionKey_hook = NULL;

static bool hypo_skip_has_subclass_hook(Oid parentOID);
static skip_has_subclass_hook_type prev_skip_has_subclass_hook = NULL;

static List *hypo_find_all_inheritors_hook(Oid parentrelID);
static find_all_inheritors_hook_type prev_find_all_inheritors_hook = NULL;

static void hypo_expand_child_rtentry_hook(PlannerInfo *root, RangeTblEntry *parentrte,
										   Index parentRTindex, Relation parentrel,
										   PlanRowMark *top_parentrc, List **appinfos,
										   PartitionDesc partdesc);
static expand_child_rtentry_hook_type prev_expand_child_rtentry_hook = NULL;

static void hypo_build_child_rtentry_hook(RangeTblEntry *childrte, Oid parentOID, Oid childOID);
static build_child_rtentry_hook_type prev_build_child_rtentry_hook = NULL;


static bool hypo_query_walker(Node *node);

void
_PG_init(void)
{
	/* Install hooks */
	prev_utility_hook = ProcessUtility_hook;
	ProcessUtility_hook = hypo_utility_hook;

	prev_ExecutorEnd_hook = ExecutorEnd_hook;
	ExecutorEnd_hook = hypo_executorEnd_hook;

	prev_get_relation_info_hook = get_relation_info_hook;
	get_relation_info_hook = hypo_get_relation_info_hook;

	prev_explain_get_index_name_hook = explain_get_index_name_hook;
	explain_get_index_name_hook = hypo_explain_get_index_name_hook;

	prev_set_rel_pathlist_hook = set_rel_pathlist_hook;
	set_rel_pathlist_hook = hypo_set_rel_pathlist_hook;

	prev_RelationGetPartitionDesc_hook = RelationGetPartitionDesc_hook;
	RelationGetPartitionDesc_hook = hypo_RelationGetPartitionDesc_hook;

	prev_RelationGetPartitionKey_hook = RelationGetPartitionKey_hook;
	RelationGetPartitionKey_hook = hypo_RelationGetPartitionKey_hook;

	prev_skip_has_subclass_hook = skip_has_subclass_hook;
	skip_has_subclass_hook = hypo_skip_has_subclass_hook;

	prev_find_all_inheritors_hook = find_all_inheritors_hook;
	find_all_inheritors_hook = hypo_find_all_inheritors_hook;

	prev_expand_child_rtentry_hook = expand_child_rtentry_hook;
	expand_child_rtentry_hook = hypo_expand_child_rtentry_hook;

	prev_build_child_rtentry_hook = build_child_rtentry_hook;
	build_child_rtentry_hook = hypo_build_child_rtentry_hook;

	isExplain = false;
	hypoIndexes = NIL;

	HypoMemoryContext = AllocSetContextCreate(TopMemoryContext,
			"HypoPG context",
#if PG_VERSION_NUM >= 90600
			ALLOCSET_DEFAULT_SIZES
#else
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE
#endif
			);

	DefineCustomBoolVariable("hypopg.enabled",
							 "Enable / Disable hypopg",
							 NULL,
							 &hypo_is_enabled,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

}

void
_PG_fini(void)
{
	/* uninstall hooks */
	ProcessUtility_hook = prev_utility_hook;
	ExecutorEnd_hook = prev_ExecutorEnd_hook;
	get_relation_info_hook = prev_get_relation_info_hook;
	explain_get_index_name_hook = prev_explain_get_index_name_hook;
	set_rel_pathlist_hook = prev_set_rel_pathlist_hook;
	RelationGetPartitionDesc_hook = prev_RelationGetPartitionDesc_hook;
	RelationGetPartitionKey_hook = prev_RelationGetPartitionKey_hook;
	skip_has_subclass_hook = prev_skip_has_subclass_hook;
	find_all_inheritors_hook = prev_find_all_inheritors_hook;
	expand_child_rtentry_hook = prev_expand_child_rtentry_hook;
	build_child_rtentry_hook = prev_build_child_rtentry_hook;
}

/*---------------------------------
 * Wrapper around GetNewRelFileNode
 * Return a new OID for an hypothetical index.
 */
Oid
hypo_getNewOid(Oid relid)
{
	Relation	pg_class;
	Relation	relation;
	Oid			newoid;
	Oid			reltablespace;
	char		relpersistence;

	/* Open the relation on which we want a new OID */
	relation = heap_open(relid, AccessShareLock);

	reltablespace = relation->rd_rel->reltablespace;
	relpersistence = relation->rd_rel->relpersistence;

	/* Close the relation and release the lock now */
	heap_close(relation, AccessShareLock);

	/* Open pg_class to aks a new OID */
	pg_class = heap_open(RelationRelationId, RowExclusiveLock);

	/* ask for a new relfilenode */
	newoid = GetNewRelFileNode(reltablespace, pg_class, relpersistence);

	/* Close pg_class and release the lock now */
	heap_close(pg_class, RowExclusiveLock);

	return newoid;
}

/* This function setup the "isExplain" flag for next hooks.
 * If this flag is setup, we can add hypothetical indexes.
 */
void
hypo_utility_hook(
#if PG_VERSION_NUM >= 100000
				  PlannedStmt *pstmt,
#else
				  Node *parsetree,
#endif
				  const char *queryString,
#if PG_VERSION_NUM >= 90300
				  ProcessUtilityContext context,
#endif
				  ParamListInfo params,
#if PG_VERSION_NUM >= 100000
				  QueryEnvironment *queryEnv,
#endif
#if PG_VERSION_NUM < 90300
				  bool isTopLevel,
#endif
				  DestReceiver *dest,
				  char *completionTag)
{
	isExplain = query_or_expression_tree_walker(
#if PG_VERSION_NUM >= 100000
						    (Node *) pstmt,
#else
						    parsetree,
#endif
						    hypo_query_walker,
						    NULL, 0);

	if (prev_utility_hook)
		prev_utility_hook(
#if PG_VERSION_NUM >= 100000
						  pstmt,
#else
						  parsetree,
#endif
						  queryString,
#if PG_VERSION_NUM >= 90300
						  context,
#endif
						  params,
#if PG_VERSION_NUM >= 100000
						  queryEnv,
#endif
#if PG_VERSION_NUM < 90300
						  isTopLevel,
#endif
						  dest, completionTag);
	else
		standard_ProcessUtility(
#if PG_VERSION_NUM >= 100000
								pstmt,
#else
								parsetree,
#endif
								queryString,
#if PG_VERSION_NUM >= 90300
								context,
#endif
								params,
#if PG_VERSION_NUM >= 100000
						  queryEnv,
#endif
#if PG_VERSION_NUM < 90300
								isTopLevel,
#endif
								dest, completionTag);

}

/* Detect if the current utility command is compatible with hypothetical indexes
 * i.e. an EXPLAIN, no ANALYZE
 */
static bool
hypo_query_walker(Node *parsetree)
{
	if (parsetree == NULL)
		return false;

#if PG_VERSION_NUM >= 100000
	parsetree = ((PlannedStmt *) parsetree)->utilityStmt;
	if (parsetree == NULL)
		return false;
#endif
	switch (nodeTag(parsetree))
	{
		case T_ExplainStmt:
			{
				ListCell   *lc;

				foreach(lc, ((ExplainStmt *) parsetree)->options)
				{
					DefElem    *opt = (DefElem *) lfirst(lc);

					if (strcmp(opt->defname, "analyze") == 0)
						return false;
				}
			}
			return true;
			break;
		default:
			return false;
	}
	return false;
}

/* Reset the isExplain flag after each query */
static void
hypo_executorEnd_hook(QueryDesc *queryDesc)
{
	isExplain = false;

	if (prev_ExecutorEnd_hook)
		prev_ExecutorEnd_hook(queryDesc);
	else
		standard_ExecutorEnd(queryDesc);
}

/*
 * This function will execute the "hypo_injectHypotheticalIndex" for every
 * hypothetical index found for each relation if the isExplain flag is setup.
 */
static void
hypo_get_relation_info_hook(PlannerInfo *root,
			    Oid relationObjectId,
			    bool inhparent,
			    RelOptInfo *rel)
{
	if (HYPO_ENABLED())
	{
		Relation	relation;

		/* Open the current relation */
		relation = heap_open(relationObjectId, AccessShareLock);

		if (relation->rd_rel->relkind == RELKIND_RELATION
#if PG_VERSION_NUM >= 90300
				|| relation->rd_rel->relkind == RELKIND_MATVIEW
#endif
				)
		{
			ListCell   *lc;

			foreach(lc, hypoIndexes)
			{
				hypoIndex  *entry = (hypoIndex *) lfirst(lc);

				if (entry->relid == relationObjectId)
				{
					/*
					 * hypothetical index found, add it to the relation's
					 * indextlist
					 */
				  hypo_injectHypotheticalIndex(root, relationObjectId,
							       inhparent, rel, relation, entry);
				}
			}
		}

		/* Close the relation release the lock now */
		heap_close(relation, AccessShareLock);

		if(hypo_table_oid_is_hypothetical(relationObjectId))
		  /*
		   * this relation is table we want to partition hypothetical,
		   * inject hypothetical partitioning
		   */
		  hypo_injectHypotheticalPartitioning(root, relationObjectId, rel);

	}
	if (prev_get_relation_info_hook)
		prev_get_relation_info_hook(root, relationObjectId, inhparent, rel);
}

/*
 * if this child relation is excluded by constraints, call set_dummy_rel_pathlist
 */
static void
hypo_set_rel_pathlist_hook(PlannerInfo *root,
						   RelOptInfo *rel,
						   Index rti,
						   RangeTblEntry *rte)
{
	if(HYPO_ENABLED() && hypo_table_oid_is_hypothetical(rte->relid)
	   && rte->relkind == 'r')
		hypo_setPartitionPathlist(root,rel,rti,rte);

	if (prev_set_rel_pathlist_hook)
		prev_set_rel_pathlist_hook(root, rel, rti, rte);
}


static PartitionDesc
hypo_RelationGetPartitionDesc_hook(Oid relid)
{
	hypoTable *table;

	if (HYPO_ENABLED() &&
		hypo_table_oid_is_hypothetical(relid))
	{
		table = hypo_find_table(relid);
		return hypo_generate_partitiondesc(table);
	}

	if (prev_RelationGetPartitionDesc_hook)
		return prev_RelationGetPartitionDesc_hook(relid);
	else
		return NULL;
}

static PartitionKey
hypo_RelationGetPartitionKey_hook(Oid relid)
{
	hypoTable *table;

	if (HYPO_ENABLED() &&
		hypo_table_oid_is_hypothetical(relid))
	{
		table = hypo_find_table(relid);
		return table->partkey;
	}

	if (prev_RelationGetPartitionKey_hook)
		return prev_RelationGetPartitionKey_hook(relid);
	else
		return NULL;
}

static bool
hypo_skip_has_subclass_hook(Oid parentOID)
{
	if (HYPO_ENABLED() && hypo_table_oid_is_hypothetical(parentOID))
		return true;

	if (prev_skip_has_subclass_hook)
		return prev_skip_has_subclass_hook(parentOID);
	else return false;
}

static List *
hypo_find_all_inheritors_hook(Oid relid)
{
	List *list;
	hypoTable *table;

	if (HYPO_ENABLED() && hypo_table_oid_is_hypothetical(relid))
	{
		table = hypo_find_table(relid);
		list = hypo_find_inheritance_children(table);
		list = lcons_oid(relid, list);
		return list;
	}

	if (prev_find_all_inheritors_hook)
		return prev_find_all_inheritors_hook(relid);
	else
		return NIL;
}


static void
hypo_expand_child_rtentry_hook(PlannerInfo *root, RangeTblEntry *parentrte,
						  Index parentRTindex, Relation parentrel,PlanRowMark *top_parentrc,
						  List **appinfos, PartitionDesc partdesc)
{
	if (HYPO_ENABLED() &&
		hypo_table_oid_is_hypothetical(parentrel->rd_id))
		hypo_ExpandChildRTE(root, parentrte, parentRTindex, parentrel,
							top_parentrc, appinfos, partdesc);

	if (prev_expand_child_rtentry_hook)
		prev_expand_child_rtentry_hook(root, parentrte, parentRTindex, parentrel,
									   top_parentrc, appinfos, partdesc);
}


static void
hypo_build_child_rtentry_hook(RangeTblEntry *childrte, Oid parentOID, Oid childOID)
{
	if (HYPO_ENABLED() &&
		hypo_table_oid_is_hypothetical(childrte->relid))
		hypo_BuildChildRTE(childrte, parentOID, childOID);

	if (prev_build_child_rtentry_hook)
		prev_build_child_rtentry_hook(childrte, parentOID, childOID);
}




/*
 * Reset all stored entries.
 */
Datum
hypopg_reset(PG_FUNCTION_ARGS)
{
	hypo_index_reset();
	hypo_table_reset();
	PG_RETURN_VOID();
}
