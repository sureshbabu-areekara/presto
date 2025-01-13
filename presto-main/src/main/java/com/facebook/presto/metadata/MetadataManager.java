/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.metadata;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.JsonCodecFactory;
import com.facebook.airlift.json.JsonObjectMapperProvider;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.TelemetryConfig;
import com.facebook.presto.common.block.BlockEncodingManager;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.telemetry.tracing.TracingEnum;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorMetadataUpdateHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorResolvedIndex;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.ConnectorViewDefinition;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.MaterializedViewDefinition;
import com.facebook.presto.spi.MaterializedViewStatus;
import com.facebook.presto.spi.NewTableLayout;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.TableLayoutFilterCoverage;
import com.facebook.presto.spi.TableMetadata;
import com.facebook.presto.spi.analyzer.MetadataResolver;
import com.facebook.presto.spi.analyzer.ViewDefinition;
import com.facebook.presto.spi.connector.ConnectorCapabilities;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorOutputMetadata;
import com.facebook.presto.spi.connector.ConnectorPartitioningHandle;
import com.facebook.presto.spi.connector.ConnectorPartitioningMetadata;
import com.facebook.presto.spi.connector.ConnectorTableVersion;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.constraints.TableConstraint;
import com.facebook.presto.spi.function.SqlFunction;
import com.facebook.presto.spi.plan.PartitioningHandle;
import com.facebook.presto.spi.security.GrantInfo;
import com.facebook.presto.spi.security.PrestoPrincipal;
import com.facebook.presto.spi.security.Privilege;
import com.facebook.presto.spi.security.RoleGrant;
import com.facebook.presto.spi.statistics.ComputedStatistics;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.facebook.presto.spi.statistics.TableStatisticsMetadata;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.analyzer.FunctionsConfig;
import com.facebook.presto.sql.analyzer.TypeSignatureProvider;
import com.facebook.presto.transaction.TransactionManager;
import com.facebook.presto.type.TypeDeserializer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.facebook.airlift.concurrent.MoreFutures.toListenableFuture;
import static com.facebook.presto.SystemSessionProperties.isIgnoreStatsCalculatorFailures;
import static com.facebook.presto.common.RuntimeMetricName.GET_LAYOUT_TIME_NANOS;
import static com.facebook.presto.common.RuntimeMetricName.GET_MATERIALIZED_VIEW_STATUS_TIME_NANOS;
import static com.facebook.presto.common.RuntimeUnit.NANO;
import static com.facebook.presto.common.function.OperatorType.BETWEEN;
import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.common.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.common.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.common.function.OperatorType.HASH_CODE;
import static com.facebook.presto.common.function.OperatorType.LESS_THAN;
import static com.facebook.presto.common.function.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.common.function.OperatorType.NOT_EQUAL;
import static com.facebook.presto.metadata.MetadataUtil.getOptionalCatalogMetadata;
import static com.facebook.presto.metadata.MetadataUtil.getOptionalTableHandle;
import static com.facebook.presto.metadata.MetadataUtil.toSchemaTableName;
import static com.facebook.presto.metadata.SessionPropertyManager.createTestingSessionPropertyManager;
import static com.facebook.presto.metadata.TableLayout.fromConnectorLayout;
import static com.facebook.presto.spi.Constraint.alwaysTrue;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_VIEW;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.StandardErrorCode.SYNTAX_ERROR;
import static com.facebook.presto.spi.TableLayoutFilterCoverage.NOT_APPLICABLE;
import static com.facebook.presto.spi.analyzer.ViewDefinition.ViewColumn;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.telemetry.TracingManager.scopedSpan;
import static com.facebook.presto.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class MetadataManager
        implements Metadata
{
    private static final Logger log = Logger.get(MetadataManager.class);

    private final FunctionAndTypeManager functionAndTypeManager;
    private final ProcedureRegistry procedures;
    private final JsonCodec<ViewDefinition> viewCodec;
    private final BlockEncodingSerde blockEncodingSerde;
    private final SessionPropertyManager sessionPropertyManager;
    private final SchemaPropertyManager schemaPropertyManager;
    private final TablePropertyManager tablePropertyManager;
    private final ColumnPropertyManager columnPropertyManager;
    private final AnalyzePropertyManager analyzePropertyManager;
    private final TransactionManager transactionManager;

    private final ConcurrentMap<String, Collection<ConnectorMetadata>> catalogsByQueryId = new ConcurrentHashMap<>();
    private final Set<QueryId> queriesWithRegisteredCallbacks = ConcurrentHashMap.newKeySet();
    private Boolean skipSpan;

    @VisibleForTesting
    public MetadataManager(
            FunctionAndTypeManager functionAndTypeManager,
            BlockEncodingSerde blockEncodingSerde,
            SessionPropertyManager sessionPropertyManager,
            SchemaPropertyManager schemaPropertyManager,
            TablePropertyManager tablePropertyManager,
            ColumnPropertyManager columnPropertyManager,
            AnalyzePropertyManager analyzePropertyManager,
            TransactionManager transactionManager)
    {
        this(
                createTestingViewCodec(functionAndTypeManager),
                blockEncodingSerde,
                sessionPropertyManager,
                schemaPropertyManager,
                tablePropertyManager,
                columnPropertyManager,
                analyzePropertyManager,
                transactionManager,
                functionAndTypeManager);
    }

    @Inject
    public MetadataManager(
            JsonCodec<ViewDefinition> viewCodec,
            BlockEncodingSerde blockEncodingSerde,
            SessionPropertyManager sessionPropertyManager,
            SchemaPropertyManager schemaPropertyManager,
            TablePropertyManager tablePropertyManager,
            ColumnPropertyManager columnPropertyManager,
            AnalyzePropertyManager analyzePropertyManager,
            TransactionManager transactionManager,
            FunctionAndTypeManager functionAndTypeManager)
    {
        this.viewCodec = requireNonNull(viewCodec, "viewCodec is null");
        this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
        this.schemaPropertyManager = requireNonNull(schemaPropertyManager, "schemaPropertyManager is null");
        this.tablePropertyManager = requireNonNull(tablePropertyManager, "tablePropertyManager is null");
        this.columnPropertyManager = requireNonNull(columnPropertyManager, "columnPropertyManager is null");
        this.analyzePropertyManager = requireNonNull(analyzePropertyManager, "analyzePropertyManager is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionManager is null");
        this.procedures = new ProcedureRegistry(functionAndTypeManager);
        this.skipSpan = TelemetryConfig.getSpanSampling();
        verifyComparableOrderableContract();
    }

    public static MetadataManager createTestMetadataManager()
    {
        return createTestMetadataManager(new FeaturesConfig());
    }

    public static MetadataManager createTestMetadataManager(FeaturesConfig featuresConfig)
    {
        return createTestMetadataManager(new CatalogManager(), featuresConfig, new FunctionsConfig());
    }

    public static MetadataManager createTestMetadataManager(FunctionsConfig functionsConfig)
    {
        return createTestMetadataManager(new CatalogManager(), new FeaturesConfig(), functionsConfig);
    }

    public static MetadataManager createTestMetadataManager(CatalogManager catalogManager)
    {
        return createTestMetadataManager(catalogManager, new FeaturesConfig(), new FunctionsConfig());
    }

    public static MetadataManager createTestMetadataManager(CatalogManager catalogManager, FeaturesConfig featuresConfig, FunctionsConfig functionsConfig)
    {
        return createTestMetadataManager(createTestTransactionManager(catalogManager), featuresConfig, functionsConfig);
    }

    public static MetadataManager createTestMetadataManager(TransactionManager transactionManager)
    {
        return createTestMetadataManager(transactionManager, new FeaturesConfig(), new FunctionsConfig());
    }

    public static MetadataManager createTestMetadataManager(TransactionManager transactionManager, FeaturesConfig featuresConfig, FunctionsConfig functionsConfig)
    {
        BlockEncodingManager blockEncodingManager = new BlockEncodingManager();
        return new MetadataManager(
                new FunctionAndTypeManager(transactionManager, blockEncodingManager, featuresConfig, functionsConfig, new HandleResolver(), ImmutableSet.of()),
                blockEncodingManager,
                createTestingSessionPropertyManager(),
                new SchemaPropertyManager(),
                new TablePropertyManager(),
                new ColumnPropertyManager(),
                new AnalyzePropertyManager(),
                transactionManager);
    }

    @Override
    public final void verifyComparableOrderableContract()
    {
        Multimap<Type, OperatorType> missingOperators = HashMultimap.create();
        for (Type type : functionAndTypeManager.getTypes()) {
            if (type.isComparable()) {
                if (!canResolveOperator(HASH_CODE, fromTypes(type))) {
                    missingOperators.put(type, HASH_CODE);
                }
                if (!canResolveOperator(EQUAL, fromTypes(type, type))) {
                    missingOperators.put(type, EQUAL);
                }
                if (!canResolveOperator(NOT_EQUAL, fromTypes(type, type))) {
                    missingOperators.put(type, NOT_EQUAL);
                }
            }
            if (type.isOrderable()) {
                for (OperatorType operator : ImmutableList.of(LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL)) {
                    if (!canResolveOperator(operator, fromTypes(type, type))) {
                        missingOperators.put(type, operator);
                    }
                }
                if (!canResolveOperator(BETWEEN, fromTypes(type, type, type))) {
                    missingOperators.put(type, BETWEEN);
                }
            }
        }
        // TODO: verify the parametric types too
        if (!missingOperators.isEmpty()) {
            List<String> messages = new ArrayList<>();
            for (Type type : missingOperators.keySet()) {
                messages.add(format("%s missing for %s", missingOperators.get(type), type));
            }
            throw new IllegalStateException(Joiner.on(", ").join(messages));
        }
    }

    @Override
    public Type getType(TypeSignature signature)
    {
        return functionAndTypeManager.getType(signature);
    }

    @Override
    public void registerBuiltInFunctions(List<? extends SqlFunction> functionInfos)
    {
        functionAndTypeManager.registerBuiltInFunctions(functionInfos);
    }

    @Override
    public List<String> listSchemaNames(Session session, String catalogName)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.LIST_SCHEMA_NAMES.getName(), skipSpan)) {
            Optional<CatalogMetadata> catalog = getOptionalCatalogMetadata(session, transactionManager, catalogName);

            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            if (catalog.isPresent()) {
                CatalogMetadata catalogMetadata = catalog.get();
                ConnectorSession connectorSession = session.toConnectorSession(catalogMetadata.getConnectorId());
                for (ConnectorId connectorId : catalogMetadata.listConnectorIds()) {
                    ConnectorMetadata metadata = catalogMetadata.getMetadataFor(connectorId);
                    metadata.listSchemaNames(connectorSession).stream()
                            .map(schema -> schema.toLowerCase(Locale.ENGLISH))
                            .forEach(schemaNames::add);
                }
            }
            return ImmutableList.copyOf(schemaNames.build());
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Optional<TableHandle> getTableHandleForStatisticsCollection(Session session, QualifiedObjectName table, Map<String, Object> analyzeProperties)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.GET_TABLE_HANDLE_FOR_STATISTICS_COLLECTION.getName(), skipSpan)) {
            requireNonNull(table, "table is null");

            Optional<CatalogMetadata> catalog = getOptionalCatalogMetadata(session, transactionManager, table.getCatalogName());
            if (catalog.isPresent()) {
                CatalogMetadata catalogMetadata = catalog.get();
                ConnectorId connectorId = catalogMetadata.getConnectorId(session, table);
                ConnectorMetadata metadata = catalogMetadata.getMetadataFor(connectorId);

                ConnectorTableHandle tableHandle = metadata.getTableHandleForStatisticsCollection(session.toConnectorSession(connectorId), toSchemaTableName(table), analyzeProperties);
                if (tableHandle != null) {
                    return Optional.of(new TableHandle(
                            connectorId,
                            tableHandle,
                            catalogMetadata.getTransactionHandleFor(connectorId),
                            Optional.empty()));
                }
            }
            return Optional.empty();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Optional<TableHandle> getHandleVersion(Session session, QualifiedObjectName tableName, Optional<ConnectorTableVersion> tableVersion)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.GET_HANDLE_VERSION.getName(), skipSpan)) {
            return getOptionalTableHandle(session, transactionManager, tableName, tableVersion);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Optional<SystemTable> getSystemTable(Session session, QualifiedObjectName tableName)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.GET_SYSTEM_TABLE.getName(), skipSpan)) {
            requireNonNull(session, "session is null");
            requireNonNull(tableName, "table is null");

            Optional<CatalogMetadata> catalog = getOptionalCatalogMetadata(session, transactionManager, tableName.getCatalogName());
            if (catalog.isPresent()) {
                CatalogMetadata catalogMetadata = catalog.get();

                // we query only main connector for runtime system tables
                ConnectorId connectorId = catalogMetadata.getConnectorId();
                ConnectorMetadata metadata = catalogMetadata.getMetadataFor(connectorId);

                return metadata.getSystemTable(session.toConnectorSession(connectorId), toSchemaTableName(tableName));
            }
            return Optional.empty();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public TableLayoutResult getLayout(Session session, TableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.GET_LAYOUT.getName(), skipSpan)) {
            long startTime = System.nanoTime();
            checkArgument(!constraint.getSummary().isNone(), "Cannot get Layout if constraint is none");

            ConnectorId connectorId = table.getConnectorId();
            ConnectorTableHandle connectorTable = table.getConnectorHandle();

            CatalogMetadata catalogMetadata = getCatalogMetadata(session, connectorId);
            ConnectorMetadata metadata = catalogMetadata.getMetadataFor(connectorId);
            ConnectorSession connectorSession = session.toConnectorSession(connectorId);
            ConnectorTableLayoutResult layout = metadata.getTableLayoutForConstraint(connectorSession, connectorTable, constraint, desiredColumns);
            session.getRuntimeStats().addMetricValue(GET_LAYOUT_TIME_NANOS, NANO, System.nanoTime() - startTime);

            return new TableLayoutResult(fromConnectorLayout(connectorId, table.getConnectorHandle(), table.getTransaction(), layout.getTableLayout()), layout.getUnenforcedConstraint());
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public TableLayout getLayout(Session session, TableHandle handle)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.GET_LAYOUT.getName(), skipSpan)) {
            ConnectorId connectorId = handle.getConnectorId();
            CatalogMetadata catalogMetadata = getCatalogMetadata(session, connectorId);
            ConnectorMetadata metadata = catalogMetadata.getMetadataFor(connectorId);
            return fromConnectorLayout(connectorId, handle.getConnectorHandle(), handle.getTransaction(), metadata.getTableLayout(session.toConnectorSession(connectorId), resolveTableLayout(session, handle)));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public TableHandle getAlternativeTableHandle(Session session, TableHandle tableHandle, PartitioningHandle partitioningHandle)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.GET_ALTERNATE_TABLE_HANDLE.getName(), skipSpan)) {
            checkArgument(partitioningHandle.getConnectorId().isPresent(), "Expect partitioning handle from connector, got system partitioning handle");
            ConnectorId connectorId = partitioningHandle.getConnectorId().get();
            checkArgument(connectorId.equals(tableHandle.getConnectorId()), "ConnectorId of tableLayoutHandle and partitioningHandle does not match");
            CatalogMetadata catalogMetadata = getCatalogMetadata(session, connectorId);
            ConnectorMetadata metadata = catalogMetadata.getMetadataFor(connectorId);
            ConnectorTableLayoutHandle newTableLayoutHandle = metadata.getAlternativeLayoutHandle(session.toConnectorSession(connectorId), tableHandle.getLayout().get(), partitioningHandle.getConnectorHandle());
            return new TableHandle(tableHandle.getConnectorId(), tableHandle.getConnectorHandle(), tableHandle.getTransaction(), Optional.of(newTableLayoutHandle));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isLegacyGetLayoutSupported(Session session, TableHandle tableHandle)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.IS_LEGACY_GET_LAYOUT_SUPPORTED.getName(), skipSpan)) {
            ConnectorId connectorId = tableHandle.getConnectorId();

            CatalogMetadata catalogMetadata = getCatalogMetadata(session, connectorId);
            ConnectorMetadata metadata = catalogMetadata.getMetadataFor(connectorId);
            return metadata.isLegacyGetLayoutSupported(session.toConnectorSession(connectorId), tableHandle.getConnectorHandle());
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Optional<PartitioningHandle> getCommonPartitioning(Session session, PartitioningHandle left, PartitioningHandle right)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.GET_COMMON_PARTITIONING.getName(), skipSpan)) {
            Optional<ConnectorId> leftConnectorId = left.getConnectorId();
            Optional<ConnectorId> rightConnectorId = right.getConnectorId();
            if (!leftConnectorId.isPresent() || !rightConnectorId.isPresent() || !leftConnectorId.equals(rightConnectorId)) {
                return Optional.empty();
            }
            if (!left.getTransactionHandle().equals(right.getTransactionHandle())) {
                return Optional.empty();
            }
            ConnectorId connectorId = leftConnectorId.get();
            CatalogMetadata catalogMetadata = getCatalogMetadata(session, connectorId);
            ConnectorMetadata metadata = catalogMetadata.getMetadataFor(connectorId);
            Optional<ConnectorPartitioningHandle> commonHandle = metadata.getCommonPartitioningHandle(session.toConnectorSession(connectorId), left.getConnectorHandle(), right.getConnectorHandle());
            return commonHandle.map(handle -> new PartitioningHandle(Optional.of(connectorId), left.getTransactionHandle(), handle));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isRefinedPartitioningOver(Session session, PartitioningHandle left, PartitioningHandle right)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.IS_REFINED_PARTITIONING_OVER.getName(), skipSpan)) {
            Optional<ConnectorId> leftConnectorId = left.getConnectorId();
            Optional<ConnectorId> rightConnectorId = right.getConnectorId();
            if (!leftConnectorId.isPresent() || !rightConnectorId.isPresent() || !leftConnectorId.equals(rightConnectorId)) {
                return false;
            }
            if (!left.getTransactionHandle().equals(right.getTransactionHandle())) {
                return false;
            }
            ConnectorId connectorId = leftConnectorId.get();
            CatalogMetadata catalogMetadata = getCatalogMetadata(session, connectorId);
            ConnectorMetadata metadata = catalogMetadata.getMetadataFor(connectorId);

            return metadata.isRefinedPartitioningOver(session.toConnectorSession(connectorId), left.getConnectorHandle(), right.getConnectorHandle());
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public PartitioningHandle getPartitioningHandleForExchange(Session session, String catalogName, int partitionCount, List<Type> partitionTypes)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.GET_PARTITIONING_HANDLE.getName(), skipSpan)) {
            CatalogMetadata catalogMetadata = getOptionalCatalogMetadata(session, transactionManager, catalogName)
                    .orElseThrow(() -> new PrestoException(NOT_FOUND, format("Catalog '%s' does not exist", catalogName)));
            ConnectorId connectorId = catalogMetadata.getConnectorId();
            ConnectorMetadata metadata = catalogMetadata.getMetadataFor(connectorId);
            ConnectorPartitioningHandle connectorPartitioningHandle = metadata.getPartitioningHandleForExchange(session.toConnectorSession(connectorId), partitionCount, partitionTypes);
            ConnectorTransactionHandle transaction = catalogMetadata.getTransactionHandleFor(connectorId);
            return new PartitioningHandle(Optional.of(connectorId), Optional.of(transaction), connectorPartitioningHandle);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Optional<Object> getInfo(Session session, TableHandle handle)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.GET_INFO.getName(), skipSpan)) {
            ConnectorId connectorId = handle.getConnectorId();
            ConnectorMetadata metadata = getMetadata(session, connectorId);
            return handle.getLayout().flatMap(tableLayout -> metadata.getInfo(tableLayout));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public TableMetadata getTableMetadata(Session session, TableHandle tableHandle)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.GET_TABLE_METADATA.getName(), ImmutableMap.of("HANDLE", tableHandle.getConnectorHandle().toString()), skipSpan)) {
            ConnectorId connectorId = tableHandle.getConnectorId();
            ConnectorMetadata metadata = getMetadata(session, connectorId);
            ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(session.toConnectorSession(connectorId), tableHandle.getConnectorHandle());
            if (tableMetadata.getColumns().isEmpty()) {
                throw new PrestoException(NOT_SUPPORTED, "Table has no columns: " + tableHandle);
            }

            return new TableMetadata(connectorId, tableMetadata);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public TableStatistics getTableStatistics(Session session, TableHandle tableHandle, List<ColumnHandle> columnHandles, Constraint<ColumnHandle> constraint)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.GET_TABLE_STATISTICS.getName(), ImmutableMap.of("HANDLE", tableHandle.getConnectorHandle().toString()), skipSpan)) {
            try {
                ConnectorId connectorId = tableHandle.getConnectorId();
                ConnectorMetadata metadata = getMetadata(session, connectorId);
                return metadata.getTableStatistics(session.toConnectorSession(connectorId), tableHandle.getConnectorHandle(), tableHandle.getLayout(), columnHandles, constraint);
            }
            catch (RuntimeException e) {
                if (isIgnoreStatsCalculatorFailures(session)) {
                    log.error(e, "Error occurred when computing stats for query %s", session.getQueryId());
                    return TableStatistics.empty();
                }
                throw e;
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(Session session, TableHandle tableHandle)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.GET_COLUMN_HANDLES.getName(), ImmutableMap.of("HANDLE", tableHandle.getConnectorHandle().toString()), skipSpan)) {
            ConnectorId connectorId = tableHandle.getConnectorId();
            ConnectorMetadata metadata = getMetadata(session, connectorId);
            Map<String, ColumnHandle> handles = metadata.getColumnHandles(session.toConnectorSession(connectorId), tableHandle.getConnectorHandle());

            ImmutableMap.Builder<String, ColumnHandle> map = ImmutableMap.builder();
            for (Entry<String, ColumnHandle> mapEntry : handles.entrySet()) {
                map.put(mapEntry.getKey().toLowerCase(ENGLISH), mapEntry.getValue());
            }
            return map.build();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ColumnMetadata getColumnMetadata(Session session, TableHandle tableHandle, ColumnHandle columnHandle)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.GET_COLUMN_METADATA.getName(), ImmutableMap.of("HANDLE", tableHandle.getConnectorHandle().toString()), skipSpan)) {
            requireNonNull(tableHandle, "tableHandle is null");
            requireNonNull(columnHandle, "columnHandle is null");

            ConnectorId connectorId = tableHandle.getConnectorId();
            ConnectorMetadata metadata = getMetadata(session, connectorId);
            return metadata.getColumnMetadata(session.toConnectorSession(connectorId), tableHandle.getConnectorHandle(), columnHandle);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public TupleDomain<ColumnHandle> toExplainIOConstraints(Session session, TableHandle tableHandle, TupleDomain<ColumnHandle> constraints)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.EXPLAIN_IO_CONSTRAINTS.getName(), ImmutableMap.of("HANDLE", tableHandle.getConnectorHandle().toString()), skipSpan)) {
            ConnectorId connectorId = tableHandle.getConnectorId();
            ConnectorMetadata metadata = getMetadata(session, connectorId);

            return metadata.toExplainIOConstraints(session.toConnectorSession(connectorId), tableHandle.getConnectorHandle(), constraints);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<QualifiedObjectName> listTables(Session session, QualifiedTablePrefix prefix)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.LIST_TABLES.getName(), ImmutableMap.of("CATALOG", prefix.getCatalogName(), "SCHEMA", prefix.getSchemaName().orElse(null), "TABLE", prefix.getTableName().orElse(null)), skipSpan)) {
            requireNonNull(prefix, "prefix is null");

            Optional<CatalogMetadata> catalog = getOptionalCatalogMetadata(session, transactionManager, prefix.getCatalogName());
            Set<QualifiedObjectName> tables = new LinkedHashSet<>();
            if (catalog.isPresent()) {
                CatalogMetadata catalogMetadata = catalog.get();

                for (ConnectorId connectorId : catalogMetadata.listConnectorIds()) {
                    ConnectorMetadata metadata = catalogMetadata.getMetadataFor(connectorId);
                    ConnectorSession connectorSession = session.toConnectorSession(connectorId);
                    metadata.listTables(connectorSession, prefix.getSchemaName()).stream()
                            .map(convertFromSchemaTableName(prefix.getCatalogName()))
                            .filter(prefix::matches)
                            .forEach(tables::add);
                }
            }
            return ImmutableList.copyOf(tables);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<QualifiedObjectName, List<ColumnMetadata>> listTableColumns(Session session, QualifiedTablePrefix prefix)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.LIST_TABLE_COLUMNS.getName(), ImmutableMap.of("CATALOG", prefix.getCatalogName(), "SCHEMA", prefix.getSchemaName().orElse(null), "TABLE", prefix.getTableName().orElse(null)), skipSpan)) {
            requireNonNull(prefix, "prefix is null");

            Optional<CatalogMetadata> catalog = getOptionalCatalogMetadata(session, transactionManager, prefix.getCatalogName());
            Map<QualifiedObjectName, List<ColumnMetadata>> tableColumns = new HashMap<>();
            if (catalog.isPresent()) {
                CatalogMetadata catalogMetadata = catalog.get();

                SchemaTablePrefix tablePrefix = prefix.asSchemaTablePrefix();
                for (ConnectorId connectorId : catalogMetadata.listConnectorIds()) {
                    ConnectorMetadata metadata = catalogMetadata.getMetadataFor(connectorId);

                    ConnectorSession connectorSession = session.toConnectorSession(connectorId);
                    for (Entry<SchemaTableName, List<ColumnMetadata>> entry : metadata.listTableColumns(connectorSession, tablePrefix).entrySet()) {
                        QualifiedObjectName tableName = new QualifiedObjectName(
                                prefix.getCatalogName(),
                                entry.getKey().getSchemaName(),
                                entry.getKey().getTableName());
                        tableColumns.put(tableName, entry.getValue());
                    }

                    // if table and view names overlap, the view wins
                    for (Entry<SchemaTableName, ConnectorViewDefinition> entry : metadata.getViews(connectorSession, tablePrefix).entrySet()) {
                        QualifiedObjectName tableName = new QualifiedObjectName(
                                prefix.getCatalogName(),
                                entry.getKey().getSchemaName(),
                                entry.getKey().getTableName());

                        ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
                        for (ViewColumn column : deserializeView(entry.getValue().getViewData()).getColumns()) {
                            columns.add(new ColumnMetadata(column.getName(), column.getType()));
                        }

                        tableColumns.put(tableName, columns.build());
                    }
                }
            }
            return ImmutableMap.copyOf(tableColumns);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void createSchema(Session session, CatalogSchemaName schema, Map<String, Object> properties)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.CREATE_SCHEMA.getName(), skipSpan)) {
            CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, schema.getCatalogName());
            ConnectorId connectorId = catalogMetadata.getConnectorId();
            ConnectorMetadata metadata = catalogMetadata.getMetadata();
            metadata.createSchema(session.toConnectorSession(connectorId), schema.getSchemaName(), properties);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void dropSchema(Session session, CatalogSchemaName schema)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.DROP_SCHEMA.getName(), ImmutableMap.of("CATALOG", schema.getCatalogName(), "SCHEMA", schema.getSchemaName()), skipSpan)) {
            CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, schema.getCatalogName());
            ConnectorId connectorId = catalogMetadata.getConnectorId();
            ConnectorMetadata metadata = catalogMetadata.getMetadata();
            metadata.dropSchema(session.toConnectorSession(connectorId), schema.getSchemaName());
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void renameSchema(Session session, CatalogSchemaName source, String target)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.RENAME_SCHEMA.getName(), ImmutableMap.of("CATALOG", source.getCatalogName(), "SCHEMA", source.getSchemaName()), skipSpan)) {
            CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, source.getCatalogName());
            ConnectorId connectorId = catalogMetadata.getConnectorId();
            ConnectorMetadata metadata = catalogMetadata.getMetadata();
            metadata.renameSchema(session.toConnectorSession(connectorId), source.getSchemaName(), target);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void createTable(Session session, String catalogName, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.CREATE_TABLE.getName(), ImmutableMap.of("CATALOG", catalogName, "SCHEMA", tableMetadata.getTable().getSchemaName(), "TABLE", tableMetadata.getTable().getTableName()), skipSpan)) {
            CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogName);
            ConnectorId connectorId = catalogMetadata.getConnectorId();
            ConnectorMetadata metadata = catalogMetadata.getMetadata();
            metadata.createTable(session.toConnectorSession(connectorId), tableMetadata, ignoreExisting);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public TableHandle createTemporaryTable(Session session, String catalogName, List<ColumnMetadata> columns, Optional<PartitioningMetadata> partitioningMetadata)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.CREATE_TEMPORARY_TABLE.getName(), ImmutableMap.of("CATALOG", catalogName), skipSpan)) {
            CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogName);
            ConnectorId connectorId = catalogMetadata.getConnectorId();
            ConnectorMetadata metadata = catalogMetadata.getMetadata();
            ConnectorTableHandle connectorTableHandle = metadata.createTemporaryTable(
                    session.toConnectorSession(connectorId),
                    columns,
                    partitioningMetadata.map(partitioning -> createConnectorPartitioningMetadata(connectorId, partitioning)));
            return new TableHandle(connectorId, connectorTableHandle, catalogMetadata.getTransactionHandleFor(connectorId), Optional.empty());
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static ConnectorPartitioningMetadata createConnectorPartitioningMetadata(ConnectorId connectorId, PartitioningMetadata partitioningMetadata)
    {
        ConnectorId partitioningConnectorId = partitioningMetadata.getPartitioningHandle().getConnectorId()
                .orElseThrow(() -> new IllegalArgumentException("connectorId is expected to be present in the connector partitioning handle"));
        checkArgument(
                connectorId.equals(partitioningConnectorId),
                "Unexpected partitioning handle connector: %s. Expected: %s.",
                partitioningConnectorId,
                connectorId);
        return new ConnectorPartitioningMetadata(partitioningMetadata.getPartitioningHandle().getConnectorHandle(), partitioningMetadata.getPartitionColumns());
    }

    @Override
    public void renameTable(Session session, TableHandle tableHandle, QualifiedObjectName newTableName)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.RENAME_TABLE.getName(), ImmutableMap.of("CATALOG", newTableName.getCatalogName(), "SCHEMA", newTableName.getSchemaName(), "TABLE", newTableName.getObjectName()), skipSpan)) {
            String catalogName = newTableName.getCatalogName();
            CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogName);
            ConnectorId connectorId = catalogMetadata.getConnectorId();
            if (!tableHandle.getConnectorId().equals(connectorId)) {
                throw new PrestoException(SYNTAX_ERROR, "Cannot rename tables across catalogs");
            }

            ConnectorMetadata metadata = catalogMetadata.getMetadata();
            metadata.renameTable(session.toConnectorSession(connectorId), tableHandle.getConnectorHandle(), toSchemaTableName(newTableName));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void setTableProperties(Session session, TableHandle tableHandle, Map<String, Object> properties)
    {
        ConnectorId connectorId = tableHandle.getConnectorId();
        ConnectorMetadata metadata = getMetadataForWrite(session, connectorId);
        metadata.setTableProperties(session.toConnectorSession(connectorId), tableHandle.getConnectorHandle(), properties);
    }

    @Override
    public void renameColumn(Session session, TableHandle tableHandle, ColumnHandle source, String target)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.RENAME_COLUMN.getName(), ImmutableMap.of("HANDLE", tableHandle.getConnectorHandle().toString()), skipSpan)) {
            ConnectorId connectorId = tableHandle.getConnectorId();
            ConnectorMetadata metadata = getMetadataForWrite(session, connectorId);
            metadata.renameColumn(session.toConnectorSession(connectorId), tableHandle.getConnectorHandle(), source, target.toLowerCase(ENGLISH));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void addColumn(Session session, TableHandle tableHandle, ColumnMetadata column)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.ADD_COLUMN.getName(), ImmutableMap.of("HANDLE", tableHandle.getConnectorHandle().toString()), skipSpan)) {
            ConnectorId connectorId = tableHandle.getConnectorId();
            ConnectorMetadata metadata = getMetadataForWrite(session, connectorId);
            metadata.addColumn(session.toConnectorSession(connectorId), tableHandle.getConnectorHandle(), column);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void dropColumn(Session session, TableHandle tableHandle, ColumnHandle column)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.DROP_COLUMN.getName(), ImmutableMap.of("HANDLE", tableHandle.getConnectorHandle().toString()), skipSpan)) {
            ConnectorId connectorId = tableHandle.getConnectorId();
            ConnectorMetadata metadata = getMetadataForWrite(session, connectorId);
            metadata.dropColumn(session.toConnectorSession(connectorId), tableHandle.getConnectorHandle(), column);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void dropTable(Session session, TableHandle tableHandle)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.DROP_TABLE.getName(), ImmutableMap.of("HANDLE", tableHandle.getConnectorHandle().toString()), skipSpan)) {
            ConnectorId connectorId = tableHandle.getConnectorId();
            ConnectorMetadata metadata = getMetadataForWrite(session, connectorId);
            metadata.dropTable(session.toConnectorSession(connectorId), tableHandle.getConnectorHandle());
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void truncateTable(Session session, TableHandle tableHandle)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.TRUNCATE_TABLE.getName(), ImmutableMap.of("HANDLE", tableHandle.getConnectorHandle().toString()), skipSpan)) {
            ConnectorId connectorId = tableHandle.getConnectorId();
            ConnectorMetadata metadata = getMetadataForWrite(session, connectorId);
            metadata.truncateTable(session.toConnectorSession(connectorId), tableHandle.getConnectorHandle());
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Optional<NewTableLayout> getInsertLayout(Session session, TableHandle table)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.GET_INSERT_LAYOUT.getName(), ImmutableMap.of("HANDLE", table.getConnectorHandle().toString()), skipSpan)) {
            ConnectorId connectorId = table.getConnectorId();
            CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, connectorId);
            ConnectorMetadata metadata = catalogMetadata.getMetadata();

            return metadata.getInsertLayout(session.toConnectorSession(connectorId), table.getConnectorHandle())
                    .map(layout -> new NewTableLayout(connectorId, catalogMetadata.getTransactionHandleFor(connectorId), layout));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public TableStatisticsMetadata getStatisticsCollectionMetadataForWrite(Session session, String catalogName, ConnectorTableMetadata tableMetadata)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.GET_STATISTICS_COLLECTION_METADATA_FOR_WRITE.getName(), ImmutableMap.of("CATALOG", catalogName, "SCHEMA", tableMetadata.getTable().getSchemaName(), "TABLE", tableMetadata.getTable().getTableName()), skipSpan)) {
            CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogName);
            ConnectorMetadata metadata = catalogMetadata.getMetadata();
            ConnectorId connectorId = catalogMetadata.getConnectorId();
            return metadata.getStatisticsCollectionMetadataForWrite(session.toConnectorSession(connectorId), tableMetadata);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public TableStatisticsMetadata getStatisticsCollectionMetadata(Session session, String catalogName, ConnectorTableMetadata tableMetadata)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.GET_STATISTICS_COLLECTION_METADATA.getName(), ImmutableMap.of("CATALOG", catalogName, "SCHEMA", tableMetadata.getTable().getSchemaName(), "TABLE", tableMetadata.getTable().getTableName()), skipSpan)) {
            CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogName);
            ConnectorMetadata metadata = catalogMetadata.getMetadata();
            ConnectorId connectorId = catalogMetadata.getConnectorId();
            return metadata.getStatisticsCollectionMetadata(session.toConnectorSession(connectorId), tableMetadata);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public AnalyzeTableHandle beginStatisticsCollection(Session session, TableHandle tableHandle)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.BEGIN_STATISTICS_COLLECTION.getName(), ImmutableMap.of("HANDLE", tableHandle.getConnectorHandle().toString()), skipSpan)) {
            ConnectorId connectorId = tableHandle.getConnectorId();
            CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, connectorId);
            ConnectorMetadata metadata = catalogMetadata.getMetadata();

            ConnectorTransactionHandle transactionHandle = catalogMetadata.getTransactionHandleFor(connectorId);
            ConnectorTableHandle connectorTableHandle = metadata.beginStatisticsCollection(session.toConnectorSession(connectorId), tableHandle.getConnectorHandle());
            return new AnalyzeTableHandle(connectorId, transactionHandle, connectorTableHandle);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void finishStatisticsCollection(Session session, AnalyzeTableHandle tableHandle, Collection<ComputedStatistics> computedStatistics)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.FINISH_STATISTICS_COLLECTION.getName(), skipSpan)) {
            ConnectorId connectorId = tableHandle.getConnectorId();
            CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, connectorId);
            catalogMetadata.getMetadata().finishStatisticsCollection(session.toConnectorSession(connectorId), tableHandle.getConnectorHandle(), computedStatistics);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Optional<NewTableLayout> getNewTableLayout(Session session, String catalogName, ConnectorTableMetadata tableMetadata)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.GET_STATISTICS_COLLECTION_METADATA.getName(), ImmutableMap.of("CATALOG", catalogName, "SCHEMA", tableMetadata.getTable().getSchemaName(), "TABLE", tableMetadata.getTable().getTableName()), skipSpan)) {
            CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogName);
            ConnectorId connectorId = catalogMetadata.getConnectorId();
            ConnectorMetadata metadata = catalogMetadata.getMetadata();

            ConnectorTransactionHandle transactionHandle = catalogMetadata.getTransactionHandleFor(connectorId);
            ConnectorSession connectorSession = session.toConnectorSession(connectorId);
            return metadata.getNewTableLayout(connectorSession, tableMetadata)
                    .map(layout -> new NewTableLayout(connectorId, transactionHandle, layout));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void beginQuery(Session session, Set<ConnectorId> connectors)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.BEGIN_QUERY.getName(), skipSpan)) {
            for (ConnectorId connectorId : connectors) {
                ConnectorMetadata metadata = getMetadata(session, connectorId);
                ConnectorSession connectorSession = session.toConnectorSession(connectorId);
                metadata.beginQuery(connectorSession);
                registerCatalogForQueryId(session.getQueryId(), metadata);
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void registerCatalogForQueryId(QueryId queryId, ConnectorMetadata metadata)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.REGISTER_CATALOG_FOR_QUERY_ID.getName(), skipSpan)) {
            catalogsByQueryId.putIfAbsent(queryId.getId(), new ArrayList<>());
            catalogsByQueryId.get(queryId.getId()).add(metadata);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void cleanupQuery(Session session)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.CLEAN_UP_QUERY.getName(), skipSpan)) {
            try {
                Collection<ConnectorMetadata> catalogs = catalogsByQueryId.get(session.getQueryId().getId());
                if (catalogs == null) {
                    return;
                }

                for (ConnectorMetadata metadata : catalogs) {
                    metadata.cleanupQuery(session.toConnectorSession());
                }
            }
            finally {
                catalogsByQueryId.remove(session.getQueryId().getId());
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public OutputTableHandle beginCreateTable(Session session, String catalogName, ConnectorTableMetadata tableMetadata, Optional<NewTableLayout> layout)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.BEGIN_CREATE_TABLE.getName(), ImmutableMap.of("CATALOG", catalogName, "SCHEMA", tableMetadata.getTable().getSchemaName(), "TABLE", tableMetadata.getTable().getTableName()), skipSpan)) {
            CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogName);
            ConnectorId connectorId = catalogMetadata.getConnectorId();
            ConnectorMetadata metadata = catalogMetadata.getMetadata();

            ConnectorTransactionHandle transactionHandle = catalogMetadata.getTransactionHandleFor(connectorId);
            ConnectorSession connectorSession = session.toConnectorSession(connectorId);
            ConnectorOutputTableHandle handle = metadata.beginCreateTable(connectorSession, tableMetadata, layout.map(NewTableLayout::getLayout));
            return new OutputTableHandle(connectorId, transactionHandle, handle);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(Session session, OutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.FINISH_CREATE_TABLE.getName(), skipSpan)) {
            ConnectorId connectorId = tableHandle.getConnectorId();
            ConnectorMetadata metadata = getMetadata(session, connectorId);
            return metadata.finishCreateTable(session.toConnectorSession(connectorId), tableHandle.getConnectorHandle(), fragments, computedStatistics);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public InsertTableHandle beginInsert(Session session, TableHandle tableHandle)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.BEGIN_INSERT.getName(), ImmutableMap.of("HANDLE", tableHandle.getConnectorHandle().toString()), skipSpan)) {
            ConnectorId connectorId = tableHandle.getConnectorId();
            CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, connectorId);
            ConnectorMetadata metadata = catalogMetadata.getMetadata();
            ConnectorTransactionHandle transactionHandle = catalogMetadata.getTransactionHandleFor(connectorId);
            ConnectorInsertTableHandle handle = metadata.beginInsert(session.toConnectorSession(connectorId), tableHandle.getConnectorHandle());
            return new InsertTableHandle(tableHandle.getConnectorId(), transactionHandle, handle);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(Session session, InsertTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.FINISH_INSERT.getName(), skipSpan)) {
            ConnectorId connectorId = tableHandle.getConnectorId();
            ConnectorMetadata metadata = getMetadata(session, connectorId);
            return metadata.finishInsert(session.toConnectorSession(connectorId), tableHandle.getConnectorHandle(), fragments, computedStatistics);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ColumnHandle getDeleteRowIdColumnHandle(Session session, TableHandle tableHandle)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.GET_DELETE_ROW_ID_COLUMN_HANDLE.getName(), ImmutableMap.of("HANDLE", tableHandle.getConnectorHandle().toString()), skipSpan)) {
            ConnectorId connectorId = tableHandle.getConnectorId();
            ConnectorMetadata metadata = getMetadata(session, connectorId);
            return metadata.getDeleteRowIdColumnHandle(session.toConnectorSession(connectorId), tableHandle.getConnectorHandle());
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ColumnHandle getUpdateRowIdColumnHandle(Session session, TableHandle tableHandle, List<ColumnHandle> updatedColumns)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.GET_UPDATE_ROW_ID_COLUMN_HANDLE.getName(), ImmutableMap.of("HANDLE", tableHandle.getConnectorHandle().toString()), skipSpan)) {
            ConnectorId connectorId = tableHandle.getConnectorId();
            ConnectorMetadata metadata = getMetadata(session, connectorId);
            return metadata.getUpdateRowIdColumnHandle(session.toConnectorSession(connectorId), tableHandle.getConnectorHandle(), updatedColumns);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean supportsMetadataDelete(Session session, TableHandle tableHandle)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.SUPPORTS_METADATA_DELETE.getName(), ImmutableMap.of("HANDLE", tableHandle.getConnectorHandle().toString()), skipSpan)) {
            ConnectorId connectorId = tableHandle.getConnectorId();
            ConnectorMetadata metadata = getMetadata(session, connectorId);
            return metadata.supportsMetadataDelete(
                    session.toConnectorSession(connectorId),
                    tableHandle.getConnectorHandle(),
                    tableHandle.getLayout());
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public OptionalLong metadataDelete(Session session, TableHandle tableHandle)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.METADATA_DELETE.getName(), ImmutableMap.of("HANDLE", tableHandle.getConnectorHandle().toString()), skipSpan)) {
            ConnectorId connectorId = tableHandle.getConnectorId();
            ConnectorMetadata metadata = getMetadataForWrite(session, connectorId);
            return metadata.metadataDelete(session.toConnectorSession(connectorId), tableHandle.getConnectorHandle(), tableHandle.getLayout().get());
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public TableHandle beginDelete(Session session, TableHandle tableHandle)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.BEGIN_DELETE.getName(), ImmutableMap.of("HANDLE", tableHandle.getConnectorHandle().toString()), skipSpan)) {
            ConnectorId connectorId = tableHandle.getConnectorId();
            CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, connectorId);
            ConnectorTableHandle newHandle = catalogMetadata.getMetadata().beginDelete(session.toConnectorSession(connectorId), tableHandle.getConnectorHandle());
            return new TableHandle(
                    tableHandle.getConnectorId(),
                    newHandle,
                    tableHandle.getTransaction(),
                    Optional.empty());
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void finishDelete(Session session, TableHandle tableHandle, Collection<Slice> fragments)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.FINISH_DELETE.getName(), ImmutableMap.of("HANDLE", tableHandle.getConnectorHandle().toString()), skipSpan)) {
            ConnectorId connectorId = tableHandle.getConnectorId();
            ConnectorMetadata metadata = getMetadata(session, connectorId);
            metadata.finishDelete(session.toConnectorSession(connectorId), tableHandle.getConnectorHandle(), fragments);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public TableHandle beginUpdate(Session session, TableHandle tableHandle, List<ColumnHandle> updatedColumns)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.BEGIN_UPDATE.getName(), ImmutableMap.of("HANDLE", tableHandle.getConnectorHandle().toString()), skipSpan)) {
            ConnectorId connectorId = tableHandle.getConnectorId();
            ConnectorMetadata metadata = getMetadataForWrite(session, connectorId);
            ConnectorTableHandle newHandle = metadata.beginUpdate(session.toConnectorSession(connectorId), tableHandle.getConnectorHandle(), updatedColumns);
            return new TableHandle(tableHandle.getConnectorId(), newHandle, tableHandle.getTransaction(), tableHandle.getLayout());
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void finishUpdate(Session session, TableHandle tableHandle, Collection<Slice> fragments)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.FINISH_UPDATE.getName(), ImmutableMap.of("HANDLE", tableHandle.getConnectorHandle().toString()), skipSpan)) {
            ConnectorId connectorId = tableHandle.getConnectorId();
            ConnectorMetadata metadata = getMetadata(session, connectorId);
            metadata.finishUpdate(session.toConnectorSession(connectorId), tableHandle.getConnectorHandle(), fragments);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Optional<ConnectorId> getCatalogHandle(Session session, String catalogName)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.GET_CATALOG_HANDLE.getName(), ImmutableMap.of("CATALOG", catalogName), skipSpan)) {
            return transactionManager.getOptionalCatalogMetadata(session.getRequiredTransactionId(), catalogName).map(CatalogMetadata::getConnectorId);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<String, ConnectorId> getCatalogNames(Session session)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.GET_CATALOG_NAMES.getName(), skipSpan)) {
            return transactionManager.getCatalogNames(session.getRequiredTransactionId());
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<QualifiedObjectName> listViews(Session session, QualifiedTablePrefix prefix)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.LIST_VIEWS.getName(), ImmutableMap.of("CATALOG", prefix.getCatalogName(), "SCHEMA", prefix.getSchemaName().orElse(null), "TABLE", prefix.getTableName().orElse(null)), skipSpan)) {
            requireNonNull(prefix, "prefix is null");

            Optional<CatalogMetadata> catalog = getOptionalCatalogMetadata(session, transactionManager, prefix.getCatalogName());

            Set<QualifiedObjectName> views = new LinkedHashSet<>();
            if (catalog.isPresent()) {
                CatalogMetadata catalogMetadata = catalog.get();

                for (ConnectorId connectorId : catalogMetadata.listConnectorIds()) {
                    ConnectorMetadata metadata = catalogMetadata.getMetadataFor(connectorId);
                    ConnectorSession connectorSession = session.toConnectorSession(connectorId);
                    metadata.listViews(connectorSession, prefix.getSchemaName()).stream()
                            .map(convertFromSchemaTableName(prefix.getCatalogName()))
                            .filter(prefix::matches)
                            .forEach(views::add);
                }
            }
            return ImmutableList.copyOf(views);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<QualifiedObjectName, ViewDefinition> getViews(Session session, QualifiedTablePrefix prefix)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.GET_VIEWS.getName(), ImmutableMap.of("CATALOG", prefix.getCatalogName(), "SCHEMA", prefix.getSchemaName().orElse(null), "TABLE", prefix.getTableName().orElse(null)), skipSpan)) {
            requireNonNull(prefix, "prefix is null");

            Optional<CatalogMetadata> catalog = getOptionalCatalogMetadata(session, transactionManager, prefix.getCatalogName());

            Map<QualifiedObjectName, ViewDefinition> views = new LinkedHashMap<>();
            if (catalog.isPresent()) {
                CatalogMetadata catalogMetadata = catalog.get();

                SchemaTablePrefix tablePrefix = prefix.asSchemaTablePrefix();
                for (ConnectorId connectorId : catalogMetadata.listConnectorIds()) {
                    ConnectorMetadata metadata = catalogMetadata.getMetadataFor(connectorId);
                    ConnectorSession connectorSession = session.toConnectorSession(connectorId);
                    for (Entry<SchemaTableName, ConnectorViewDefinition> entry : metadata.getViews(connectorSession, tablePrefix).entrySet()) {
                        QualifiedObjectName viewName = new QualifiedObjectName(
                                prefix.getCatalogName(),
                                entry.getKey().getSchemaName(),
                                entry.getKey().getTableName());
                        views.put(viewName, deserializeView(entry.getValue().getViewData()));
                    }
                }
            }
            return ImmutableMap.copyOf(views);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void createView(Session session, String catalogName, ConnectorTableMetadata viewMetadata, String viewData, boolean replace)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.CREATE_VIEW.getName(), ImmutableMap.of("CATALOG", catalogName, "SCHEMA", viewMetadata.getTable().getSchemaName(), "TABLE", viewMetadata.getTable().getTableName()), skipSpan)) {
            CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogName);
            ConnectorId connectorId = catalogMetadata.getConnectorId();
            ConnectorMetadata metadata = catalogMetadata.getMetadata();

            metadata.createView(session.toConnectorSession(connectorId), viewMetadata, viewData, replace);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void dropView(Session session, QualifiedObjectName viewName)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.DROP_VIEW.getName(), ImmutableMap.of("CATALOG", viewName.getCatalogName(), "SCHEMA", viewName.getSchemaName(), "TABLE", viewName.getObjectName()), skipSpan)) {
            CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, viewName.getCatalogName());
            ConnectorId connectorId = catalogMetadata.getConnectorId();
            ConnectorMetadata metadata = catalogMetadata.getMetadata();

            metadata.dropView(session.toConnectorSession(connectorId), toSchemaTableName(viewName));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void createMaterializedView(Session session, String catalogName, ConnectorTableMetadata viewMetadata, MaterializedViewDefinition viewDefinition, boolean ignoreExisting)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.CREATE_MATERIALIZED_VIEW.getName(), ImmutableMap.of("CATALOG", catalogName, "SCHEMA", viewMetadata.getTable().getSchemaName(), "TABLE", viewMetadata.getTable().getTableName()), skipSpan)) {
            CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogName);
            ConnectorId connectorId = catalogMetadata.getConnectorId();
            ConnectorMetadata metadata = catalogMetadata.getMetadata();

            metadata.createMaterializedView(session.toConnectorSession(connectorId), viewMetadata, viewDefinition, ignoreExisting);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void dropMaterializedView(Session session, QualifiedObjectName viewName)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.DROP_MATERIALIZED_VIEW.getName(), ImmutableMap.of("CATALOG", viewName.getCatalogName(), "SCHEMA", viewName.getSchemaName(), "TABLE", viewName.getObjectName()), skipSpan)) {
            CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, viewName.getCatalogName());
            ConnectorId connectorId = catalogMetadata.getConnectorId();
            ConnectorMetadata metadata = catalogMetadata.getMetadata();

            metadata.dropMaterializedView(session.toConnectorSession(connectorId), toSchemaTableName(viewName));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private MaterializedViewStatus getMaterializedViewStatus(Session session, QualifiedObjectName materializedViewName, TupleDomain<String> baseQueryDomain)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.GET_MATERIALIZED_VIEW_STATUS.getName(), ImmutableMap.of("CATALOG", materializedViewName.getCatalogName(), "SCHEMA", materializedViewName.getSchemaName(), "TABLE", materializedViewName.getObjectName()), skipSpan)) {
            Optional<TableHandle> materializedViewHandle = getOptionalTableHandle(session, transactionManager, materializedViewName, Optional.empty());

            ConnectorId connectorId = materializedViewHandle.get().getConnectorId();
            ConnectorMetadata metadata = getMetadata(session, connectorId);

            return session.getRuntimeStats().profileNanos(
                    GET_MATERIALIZED_VIEW_STATUS_TIME_NANOS,
                    () -> metadata.getMaterializedViewStatus(session.toConnectorSession(connectorId), toSchemaTableName(materializedViewName), baseQueryDomain));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public InsertTableHandle beginRefreshMaterializedView(Session session, TableHandle tableHandle)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.BEGIN_REFRESH_MATERIALIZED_VIEW.getName(), ImmutableMap.of("HANDLE", tableHandle.getConnectorHandle().toString()), skipSpan)) {
            ConnectorId connectorId = tableHandle.getConnectorId();
            CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, connectorId);
            ConnectorMetadata metadata = catalogMetadata.getMetadata();
            ConnectorTransactionHandle transactionHandle = catalogMetadata.getTransactionHandleFor(connectorId);
            ConnectorInsertTableHandle handle = metadata.beginRefreshMaterializedView(session.toConnectorSession(connectorId), tableHandle.getConnectorHandle());
            return new InsertTableHandle(tableHandle.getConnectorId(), transactionHandle, handle);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishRefreshMaterializedView(Session session, InsertTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.FINISH_REFRESH_MATERIALIZED_VIEW.getName(), skipSpan)) {
            ConnectorId connectorId = tableHandle.getConnectorId();
            ConnectorMetadata metadata = getMetadata(session, connectorId);
            return metadata.finishRefreshMaterializedView(session.toConnectorSession(connectorId), tableHandle.getConnectorHandle(), fragments, computedStatistics);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<QualifiedObjectName> getReferencedMaterializedViews(Session session, QualifiedObjectName tableName)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.GET_REFERENCED_MATERIALIZED_VIEWS.getName(), ImmutableMap.of("CATALOG", tableName.getCatalogName(), "SCHEMA", tableName.getSchemaName(), "TABLE", tableName.getObjectName()), skipSpan)) {
            requireNonNull(tableName, "tableName is null");

            Optional<CatalogMetadata> catalog = getOptionalCatalogMetadata(session, transactionManager, tableName.getCatalogName());
            if (catalog.isPresent()) {
                ConnectorMetadata metadata = catalog.get().getMetadata();
                ConnectorSession connectorSession = session.toConnectorSession(catalog.get().getConnectorId());
                Optional<List<SchemaTableName>> materializedViews = metadata.getReferencedMaterializedViews(connectorSession, toSchemaTableName(tableName));
                if (materializedViews.isPresent()) {
                    return materializedViews.get().stream().map(convertFromSchemaTableName(tableName.getCatalogName())).collect(toImmutableList());
                }
            }
            return ImmutableList.of();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Optional<ResolvedIndex> resolveIndex(Session session, TableHandle tableHandle, Set<ColumnHandle> indexableColumns, Set<ColumnHandle> outputColumns, TupleDomain<ColumnHandle> tupleDomain)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.RESOLVE_INDEX.getName(), ImmutableMap.of("HANDLE", tableHandle.getConnectorHandle().toString()), skipSpan)) {
            ConnectorId connectorId = tableHandle.getConnectorId();
            CatalogMetadata catalogMetadata = getCatalogMetadata(session, connectorId);
            ConnectorMetadata metadata = catalogMetadata.getMetadataFor(connectorId);
            ConnectorTransactionHandle transaction = catalogMetadata.getTransactionHandleFor(connectorId);
            ConnectorSession connectorSession = session.toConnectorSession(connectorId);
            Optional<ConnectorResolvedIndex> resolvedIndex = metadata.resolveIndex(connectorSession, tableHandle.getConnectorHandle(), indexableColumns, outputColumns, tupleDomain);
            return resolvedIndex.map(resolved -> new ResolvedIndex(tableHandle.getConnectorId(), transaction, resolved));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void createRole(Session session, String role, Optional<PrestoPrincipal> grantor, String catalog)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.CREATE_ROLE.getName(), skipSpan)) {
            CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalog);
            ConnectorId connectorId = catalogMetadata.getConnectorId();
            ConnectorMetadata metadata = catalogMetadata.getMetadata();

            metadata.createRole(session.toConnectorSession(connectorId), role, grantor);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void dropRole(Session session, String role, String catalog)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.DROP_ROLE.getName(), skipSpan)) {
            CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalog);
            ConnectorId connectorId = catalogMetadata.getConnectorId();
            ConnectorMetadata metadata = catalogMetadata.getMetadata();

            metadata.dropRole(session.toConnectorSession(connectorId), role);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Set<String> listRoles(Session session, String catalog)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.LIST_ROLES.getName(), skipSpan)) {
            Optional<CatalogMetadata> catalogMetadata = getOptionalCatalogMetadata(session, transactionManager, catalog);
            if (!catalogMetadata.isPresent()) {
                return ImmutableSet.of();
            }
            ConnectorId connectorId = catalogMetadata.get().getConnectorId();
            ConnectorSession connectorSession = session.toConnectorSession(connectorId);
            ConnectorMetadata metadata = catalogMetadata.get().getMetadataFor(connectorId);
            return metadata.listRoles(connectorSession).stream()
                    .map(role -> role.toLowerCase(ENGLISH))
                    .collect(toImmutableSet());
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Set<RoleGrant> listRoleGrants(Session session, String catalog, PrestoPrincipal principal)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.LIST_ROLE_GRANTS.getName(), skipSpan)) {
            Optional<CatalogMetadata> catalogMetadata = getOptionalCatalogMetadata(session, transactionManager, catalog);
            if (!catalogMetadata.isPresent()) {
                return ImmutableSet.of();
            }
            ConnectorId connectorId = catalogMetadata.get().getConnectorId();
            ConnectorSession connectorSession = session.toConnectorSession(connectorId);
            ConnectorMetadata metadata = catalogMetadata.get().getMetadataFor(connectorId);
            return metadata.listRoleGrants(connectorSession, principal);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void grantRoles(Session session, Set<String> roles, Set<PrestoPrincipal> grantees, boolean withAdminOption, Optional<PrestoPrincipal> grantor, String catalog)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.GRANT_ROLES.getName(), skipSpan)) {
            CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalog);
            ConnectorId connectorId = catalogMetadata.getConnectorId();
            ConnectorMetadata metadata = catalogMetadata.getMetadata();

            metadata.grantRoles(session.toConnectorSession(connectorId), roles, grantees, withAdminOption, grantor);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void revokeRoles(Session session, Set<String> roles, Set<PrestoPrincipal> grantees, boolean adminOptionFor, Optional<PrestoPrincipal> grantor, String catalog)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.REVOKE_ROLES.getName(), skipSpan)) {
            CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalog);
            ConnectorId connectorId = catalogMetadata.getConnectorId();
            ConnectorMetadata metadata = catalogMetadata.getMetadata();

            metadata.revokeRoles(session.toConnectorSession(connectorId), roles, grantees, adminOptionFor, grantor);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Set<RoleGrant> listApplicableRoles(Session session, PrestoPrincipal principal, String catalog)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.LIST_APPLICABLE_ROLES.getName(), skipSpan)) {
            Optional<CatalogMetadata> catalogMetadata = getOptionalCatalogMetadata(session, transactionManager, catalog);
            if (!catalogMetadata.isPresent()) {
                return ImmutableSet.of();
            }
            ConnectorId connectorId = catalogMetadata.get().getConnectorId();
            ConnectorSession connectorSession = session.toConnectorSession(connectorId);
            ConnectorMetadata metadata = catalogMetadata.get().getMetadataFor(connectorId);
            return ImmutableSet.copyOf(metadata.listApplicableRoles(connectorSession, principal));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Set<String> listEnabledRoles(Session session, String catalog)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.LIST_ENABLED_ROLES.getName(), skipSpan)) {
            Optional<CatalogMetadata> catalogMetadata = getOptionalCatalogMetadata(session, transactionManager, catalog);
            if (!catalogMetadata.isPresent()) {
                return ImmutableSet.of();
            }
            ConnectorId connectorId = catalogMetadata.get().getConnectorId();
            ConnectorSession connectorSession = session.toConnectorSession(connectorId);
            ConnectorMetadata metadata = catalogMetadata.get().getMetadataFor(connectorId);
            return ImmutableSet.copyOf(metadata.listEnabledRoles(connectorSession));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void grantTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, PrestoPrincipal grantee, boolean grantOption)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.GRANT_TABLE_PRIVILEGES.getName(), ImmutableMap.of("CATALOG", tableName.getCatalogName(), "SCHEMA", tableName.getSchemaName(), "TABLE", tableName.getObjectName()), skipSpan)) {
            CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, tableName.getCatalogName());
            ConnectorId connectorId = catalogMetadata.getConnectorId();
            ConnectorMetadata metadata = catalogMetadata.getMetadata();

            metadata.grantTablePrivileges(session.toConnectorSession(connectorId), toSchemaTableName(tableName), privileges, grantee, grantOption);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void revokeTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, PrestoPrincipal grantee, boolean grantOption)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.REVOKE_TABLE_PRIVILEGES.getName(), ImmutableMap.of("CATALOG", tableName.getCatalogName(), "SCHEMA", tableName.getSchemaName(), "TABLE", tableName.getObjectName()), skipSpan)) {
            CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, tableName.getCatalogName());
            ConnectorId connectorId = catalogMetadata.getConnectorId();
            ConnectorMetadata metadata = catalogMetadata.getMetadata();

            metadata.revokeTablePrivileges(session.toConnectorSession(connectorId), toSchemaTableName(tableName), privileges, grantee, grantOption);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<GrantInfo> listTablePrivileges(Session session, QualifiedTablePrefix prefix)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.LIST_TABLE_PRIVILEGES.getName(), ImmutableMap.of("CATALOG", prefix.getCatalogName(), "SCHEMA", prefix.getSchemaName().orElse(null), "TABLE", prefix.getTableName().orElse(null)), skipSpan)) {
            requireNonNull(prefix, "prefix is null");
            SchemaTablePrefix tablePrefix = prefix.asSchemaTablePrefix();

            Optional<CatalogMetadata> catalog = getOptionalCatalogMetadata(session, transactionManager, prefix.getCatalogName());

            ImmutableSet.Builder<GrantInfo> grantInfos = ImmutableSet.builder();
            if (catalog.isPresent()) {
                CatalogMetadata catalogMetadata = catalog.get();
                ConnectorSession connectorSession = session.toConnectorSession(catalogMetadata.getConnectorId());
                for (ConnectorId connectorId : catalogMetadata.listConnectorIds()) {
                    ConnectorMetadata metadata = catalogMetadata.getMetadataFor(connectorId);
                    grantInfos.addAll(metadata.listTablePrivileges(connectorSession, tablePrefix));
                }
            }
            return ImmutableList.copyOf(grantInfos.build());
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ListenableFuture<Void> commitPageSinkAsync(Session session, OutputTableHandle tableHandle, Collection<Slice> fragments)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.COMMIT_PAGE_SINK_ASYNC.getName(), skipSpan)) {
            ConnectorId connectorId = tableHandle.getConnectorId();
            CatalogMetadata catalogMetadata = getCatalogMetadata(session, connectorId);
            ConnectorMetadata metadata = catalogMetadata.getMetadata();
            ConnectorSession connectorSession = session.toConnectorSession(connectorId);

            return toListenableFuture(metadata.commitPageSinkAsync(connectorSession, tableHandle.getConnectorHandle(), fragments));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ListenableFuture<Void> commitPageSinkAsync(Session session, InsertTableHandle tableHandle, Collection<Slice> fragments)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.COMMIT_PAGE_SINK_ASYNC.getName(), skipSpan)) {
            ConnectorId connectorId = tableHandle.getConnectorId();
            CatalogMetadata catalogMetadata = getCatalogMetadata(session, connectorId);
            ConnectorMetadata metadata = catalogMetadata.getMetadata();
            ConnectorSession connectorSession = session.toConnectorSession(connectorId);

            return toListenableFuture(metadata.commitPageSinkAsync(connectorSession, tableHandle.getConnectorHandle(), fragments));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public MetadataUpdates getMetadataUpdateResults(Session session, QueryManager queryManager, MetadataUpdates metadataUpdateRequests, QueryId queryId)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.GET_METADATA_UPDATE_RESULTS.getName(), skipSpan)) {
            ConnectorId connectorId = metadataUpdateRequests.getConnectorId();
            ConnectorMetadata metadata = getCatalogMetadata(session, connectorId).getMetadata();

            if (queryManager != null && !queriesWithRegisteredCallbacks.contains(queryId)) {
                // This is the first time we are getting requests for queryId.
                // Register a callback, so the we do the cleanup when query fails/finishes.
                queryManager.addStateChangeListener(queryId, state -> {
                    if (state.isDone()) {
                        metadata.doMetadataUpdateCleanup(queryId);
                        queriesWithRegisteredCallbacks.remove(queryId);
                    }
                });
                queriesWithRegisteredCallbacks.add(queryId);
            }

            List<ConnectorMetadataUpdateHandle> metadataResults = metadata.getMetadataUpdateResults(metadataUpdateRequests.getMetadataUpdates(), queryId);
            return new MetadataUpdates(connectorId, metadataResults);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public FunctionAndTypeManager getFunctionAndTypeManager()
    {
        // TODO: transactional when FunctionManager is made transactional
        return functionAndTypeManager;
    }

    @Override
    public ProcedureRegistry getProcedureRegistry()
    {
        return procedures;
    }

    @Override
    public BlockEncodingSerde getBlockEncodingSerde()
    {
        return blockEncodingSerde;
    }

    @Override
    public SessionPropertyManager getSessionPropertyManager()
    {
        return sessionPropertyManager;
    }

    @Override
    public SchemaPropertyManager getSchemaPropertyManager()
    {
        return schemaPropertyManager;
    }

    @Override
    public TablePropertyManager getTablePropertyManager()
    {
        return tablePropertyManager;
    }

    @Override
    public ColumnPropertyManager getColumnPropertyManager()
    {
        return columnPropertyManager;
    }

    public AnalyzePropertyManager getAnalyzePropertyManager()
    {
        return analyzePropertyManager;
    }

    @Override
    public MetadataResolver getMetadataResolver(Session session)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.GET_METADATA_RESOLVER.getName(), skipSpan)) {
            return new MetadataResolver()
            {
                @Override
                public boolean catalogExists(String catalogName)
                {
                    try (AutoCloseable ignored = scopedSpan(TracingEnum.CATALOG_EXISTS.getName(), ImmutableMap.of("CATALOG", catalogName), skipSpan)) {
                        return getOptionalCatalogMetadata(session, transactionManager, catalogName).isPresent();
                    }
                    catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }

                @Override
                public boolean schemaExists(CatalogSchemaName schema)
                {
                    try (AutoCloseable ignored = scopedSpan(TracingEnum.SCHEMA_EXISTS.getName(), ImmutableMap.of("CATALOG", schema.getCatalogName(), "SCHEMA", schema.getSchemaName()), skipSpan)) {
                        Optional<CatalogMetadata> catalog = getOptionalCatalogMetadata(session, transactionManager, schema.getCatalogName());
                        if (!catalog.isPresent()) {
                            return false;
                        }
                        CatalogMetadata catalogMetadata = catalog.get();
                        ConnectorSession connectorSession = session.toConnectorSession(catalogMetadata.getConnectorId());
                        return catalogMetadata.listConnectorIds().stream()
                                .map(catalogMetadata::getMetadataFor)
                                .anyMatch(metadata -> metadata.schemaExists(connectorSession, schema.getSchemaName()));
                    }
                    catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }

                @Override
                public boolean tableExists(QualifiedObjectName tableName)
                {
                    try (AutoCloseable ignored = scopedSpan(TracingEnum.TABLE_EXISTS.getName(), ImmutableMap.of("CATALOG", tableName.getCatalogName(), "SCHEMA", tableName.getSchemaName(), "TABLE", tableName.getObjectName()), skipSpan)) {
                        return getOptionalTableHandle(session, transactionManager, tableName, Optional.empty()).isPresent();
                    }
                    catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }

                @Override
                public Optional<TableHandle> getTableHandle(QualifiedObjectName tableName)
                {
                    try (AutoCloseable ignored = scopedSpan(TracingEnum.GET_TABLE_HANDLE.getName(), ImmutableMap.of("CATALOG", tableName.getCatalogName(), "SCHEMA", tableName.getSchemaName(), "TABLE", tableName.getObjectName()), skipSpan)) {
                        return getOptionalTableHandle(session, transactionManager, tableName, Optional.empty());
                    }
                    catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }

                @Override
                public List<ColumnMetadata> getColumns(TableHandle tableHandle)
                {
                    try (AutoCloseable ignored = scopedSpan(TracingEnum.GET_COLUMNS.getName(), skipSpan)) {
                        return getTableMetadata(session, tableHandle).getColumns();
                    }
                    catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }

                @Override
                public Map<String, ColumnHandle> getColumnHandles(TableHandle tableHandle)
                {
                    try (AutoCloseable ignored = scopedSpan(TracingEnum.GET_COLUMN_HANDLES.getName(), skipSpan)) {
                        return MetadataManager.this.getColumnHandles(session, tableHandle);
                    }
                    catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }

                @Override
                public Optional<ViewDefinition> getView(QualifiedObjectName viewName)
                {
                    try (AutoCloseable ignored = scopedSpan(TracingEnum.GET_VIEW.getName(), ImmutableMap.of("CATALOG", viewName.getCatalogName(), "SCHEMA", viewName.getSchemaName(), "TABLE", viewName.getObjectName()), skipSpan)) {
                        Optional<CatalogMetadata> catalog = getOptionalCatalogMetadata(session, transactionManager, viewName.getCatalogName());
                        if (catalog.isPresent()) {
                            CatalogMetadata catalogMetadata = catalog.get();
                            ConnectorId connectorId = catalogMetadata.getConnectorId(session, viewName);
                            ConnectorMetadata metadata = catalogMetadata.getMetadataFor(connectorId);

                            Map<SchemaTableName, ConnectorViewDefinition> views = metadata.getViews(
                                    session.toConnectorSession(connectorId),
                                    toSchemaTableName(viewName).toSchemaTablePrefix());
                            ConnectorViewDefinition view = views.get(toSchemaTableName(viewName));
                            if (view != null) {
                                ViewDefinition definition = deserializeView(view.getViewData());
                                if (view.getOwner().isPresent() && !definition.isRunAsInvoker()) {
                                    definition = definition.withOwner(view.getOwner().get());
                                }
                                return Optional.of(definition);
                            }
                        }
                        return Optional.empty();
                    }
                    catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }

                @Override
                public Optional<MaterializedViewDefinition> getMaterializedView(QualifiedObjectName viewName)
                {
                    try (AutoCloseable ignored = scopedSpan(TracingEnum.GET_MATERIALIZED_VIEW.getName(), ImmutableMap.of("CATALOG", viewName.getCatalogName(), "SCHEMA", viewName.getSchemaName(), "TABLE", viewName.getObjectName()), skipSpan)) {
                        Optional<CatalogMetadata> catalog = getOptionalCatalogMetadata(session, transactionManager, viewName.getCatalogName());
                        if (catalog.isPresent()) {
                            CatalogMetadata catalogMetadata = catalog.get();
                            ConnectorId connectorId = catalogMetadata.getConnectorId(session, viewName);
                            ConnectorMetadata metadata = catalogMetadata.getMetadataFor(connectorId);

                            return metadata.getMaterializedView(session.toConnectorSession(connectorId), toSchemaTableName(viewName));
                        }
                        return Optional.empty();
                    }
                    catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }

                @Override
                public MaterializedViewStatus getMaterializedViewStatus(QualifiedObjectName materializedViewName, TupleDomain<String> baseQueryDomain)
                {
                    try (AutoCloseable ignored = scopedSpan(TracingEnum.GET_MATERIALIZED_VIEW_STATUS.getName(), ImmutableMap.of("CATALOG", materializedViewName.getCatalogName(), "SCHEMA", materializedViewName.getSchemaName(), "TABLE", materializedViewName.getObjectName()), skipSpan)) {
                        return MetadataManager.this.getMaterializedViewStatus(session, materializedViewName, baseQueryDomain);
                    }
                    catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            };
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Set<ConnectorCapabilities> getConnectorCapabilities(Session session, ConnectorId connectorId)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.GET_CONNECTOR_CAPABILITIES.getName(), skipSpan)) {
            return getCatalogMetadata(session, connectorId).getConnectorCapabilities();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public TableLayoutFilterCoverage getTableLayoutFilterCoverage(Session session, TableHandle tableHandle, Set<String> relevantPartitionColumns)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.GET_TABLE_LAYOUT_FILTER_COVERAGE.getName(), ImmutableMap.of("HANDLE", tableHandle.getConnectorHandle().toString()), skipSpan)) {
            requireNonNull(tableHandle, "tableHandle cannot be null");
            requireNonNull(relevantPartitionColumns, "relevantPartitionKeys cannot be null");

            if (!tableHandle.getLayout().isPresent()) {
                return NOT_APPLICABLE;
            }

            ConnectorId connectorId = tableHandle.getConnectorId();
            CatalogMetadata catalogMetadata = getCatalogMetadata(session, connectorId);
            ConnectorMetadata metadata = catalogMetadata.getMetadataFor(connectorId);
            return metadata.getTableLayoutFilterCoverage(tableHandle.getLayout().get(), relevantPartitionColumns);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void dropConstraint(Session session, TableHandle tableHandle, Optional<String> constraintName, Optional<String> columnName)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.DROP_CONSTRAINT.getName(), ImmutableMap.of("HANDLE", tableHandle.getConnectorHandle().toString()), skipSpan)) {
            ConnectorId connectorId = tableHandle.getConnectorId();
            ConnectorMetadata metadata = getMetadataForWrite(session, connectorId);
            metadata.dropConstraint(session.toConnectorSession(connectorId), tableHandle.getConnectorHandle(), constraintName, columnName);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void addConstraint(Session session, TableHandle tableHandle, TableConstraint<String> tableConstraint)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.ADD_CONSTRAINT.getName(), ImmutableMap.of("HANDLE", tableHandle.getConnectorHandle().toString()), skipSpan)) {
            ConnectorId connectorId = tableHandle.getConnectorId();
            ConnectorMetadata metadata = getMetadataForWrite(session, connectorId);
            metadata.addConstraint(session.toConnectorSession(connectorId), tableHandle.getConnectorHandle(), tableConstraint);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private ViewDefinition deserializeView(String data)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.DESERIALIZE_VIEW.getName(), skipSpan)) {
            try {
                return viewCodec.fromJson(data);
            }
            catch (IllegalArgumentException e) {
                throw new PrestoException(INVALID_VIEW, "Invalid view JSON: " + data, e);
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private CatalogMetadata getCatalogMetadata(Session session, ConnectorId connectorId)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.GET_CATALOG_METADATA.getName(), skipSpan)) {
            return transactionManager.getCatalogMetadata(session.getRequiredTransactionId(), connectorId);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private CatalogMetadata getCatalogMetadataForWrite(Session session, String catalogName)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.GET_CATALOG_METADATA_FOR_WRITE.getName(), ImmutableMap.of("CATALOG", catalogName), skipSpan)) {
            return transactionManager.getCatalogMetadataForWrite(session.getRequiredTransactionId(), catalogName);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private CatalogMetadata getCatalogMetadataForWrite(Session session, ConnectorId connectorId)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.GET_CATALOG_METADATA_FOR_WRITE.getName(), skipSpan)) {
            return transactionManager.getCatalogMetadataForWrite(session.getRequiredTransactionId(), connectorId);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private ConnectorMetadata getMetadata(Session session, ConnectorId connectorId)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.GET_METADATA.getName(), skipSpan)) {
            return getCatalogMetadata(session, connectorId).getMetadataFor(connectorId);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private ConnectorMetadata getMetadataForWrite(Session session, ConnectorId connectorId)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.GET_METADATA_FOR_WRITE.getName(), skipSpan)) {
            return getCatalogMetadataForWrite(session, connectorId).getMetadata();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static JsonCodec<ViewDefinition> createTestingViewCodec(FunctionAndTypeManager functionAndTypeManager)
    {
        JsonObjectMapperProvider provider = new JsonObjectMapperProvider();
        provider.setJsonDeserializers(ImmutableMap.of(Type.class, new TypeDeserializer(functionAndTypeManager)));
        return new JsonCodecFactory(provider).jsonCodec(ViewDefinition.class);
    }

    private boolean canResolveOperator(OperatorType operatorType, List<TypeSignatureProvider> argumentTypes)
    {
        try {
            getFunctionAndTypeManager().resolveOperator(operatorType, argumentTypes);
            return true;
        }
        catch (OperatorNotFoundException e) {
            return false;
        }
    }

    private ConnectorTableLayoutHandle resolveTableLayout(Session session, TableHandle tableHandle)
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.RESOLVE_TABLE_LAYOUT.getName(), ImmutableMap.of("HANDLE", tableHandle.getConnectorHandle().toString()), skipSpan)) {
            if (tableHandle.getLayout().isPresent()) {
                return tableHandle.getLayout().get();
            }
            TableLayoutResult result = getLayout(session, tableHandle, alwaysTrue(), Optional.empty());
            return result.getLayout().getLayoutHandle();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @VisibleForTesting
    public Map<String, Collection<ConnectorMetadata>> getCatalogsByQueryId()
    {
        try (AutoCloseable ignored = scopedSpan(TracingEnum.GET_CATALOGS_BY_QUERY_ID.getName(), skipSpan)) {
            return ImmutableMap.copyOf(catalogsByQueryId);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Function<SchemaTableName, QualifiedObjectName> convertFromSchemaTableName(String catalogName)
    {
        return input -> new QualifiedObjectName(catalogName, input.getSchemaName(), input.getTableName());
    }
}
