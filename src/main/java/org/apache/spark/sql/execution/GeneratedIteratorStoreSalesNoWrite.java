package org.apache.spark.sql.execution;

import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.types.Decimal;

/**
 * Created by atr on 25.10.17.
 */

/* 005 */ public final class GeneratedIteratorStoreSalesNoWrite extends org.apache.spark.sql.execution.BufferedRowIterator {
                private long _sum = 0;
    private long _validDecimal = 0;
    /* 006 */   private Object[] references;
    /* 007 */   private scala.collection.Iterator[] inputs;
    /* 008 */   private scala.collection.Iterator scan_input;
    /* 009 */   private org.apache.spark.sql.execution.metric.SQLMetric scan_numOutputRows;
    /* 010 */   private org.apache.spark.sql.execution.metric.SQLMetric scan_scanTime;
    /* 011 */   private long scan_scanTime1;
    /* 012 */   private org.apache.spark.sql.execution.vectorized.ColumnarBatch scan_batch;
    /* 013 */   private int scan_batchIdx;
    /* 014 */   private org.apache.spark.sql.execution.vectorized.ColumnVector scan_colInstance0;
    /* 015 */   private org.apache.spark.sql.execution.vectorized.ColumnVector scan_colInstance1;
    /* 016 */   private org.apache.spark.sql.execution.vectorized.ColumnVector scan_colInstance2;
    /* 017 */   private org.apache.spark.sql.execution.vectorized.ColumnVector scan_colInstance3;
    /* 018 */   private org.apache.spark.sql.execution.vectorized.ColumnVector scan_colInstance4;
    /* 019 */   private org.apache.spark.sql.execution.vectorized.ColumnVector scan_colInstance5;
    /* 020 */   private org.apache.spark.sql.execution.vectorized.ColumnVector scan_colInstance6;
    /* 021 */   private org.apache.spark.sql.execution.vectorized.ColumnVector scan_colInstance7;
    /* 022 */   private org.apache.spark.sql.execution.vectorized.ColumnVector scan_colInstance8;
    /* 023 */   private org.apache.spark.sql.execution.vectorized.ColumnVector scan_colInstance9;
    /* 024 */   private org.apache.spark.sql.execution.vectorized.ColumnVector scan_colInstance10;
    /* 025 */   private org.apache.spark.sql.execution.vectorized.ColumnVector scan_colInstance11;
    /* 026 */   private org.apache.spark.sql.execution.vectorized.ColumnVector scan_colInstance12;
    /* 027 */   private org.apache.spark.sql.execution.vectorized.ColumnVector scan_colInstance13;
    /* 028 */   private org.apache.spark.sql.execution.vectorized.ColumnVector scan_colInstance14;
    /* 029 */   private org.apache.spark.sql.execution.vectorized.ColumnVector scan_colInstance15;
    /* 030 */   private org.apache.spark.sql.execution.vectorized.ColumnVector scan_colInstance16;
    /* 031 */   private org.apache.spark.sql.execution.vectorized.ColumnVector scan_colInstance17;
    /* 032 */   private org.apache.spark.sql.execution.vectorized.ColumnVector scan_colInstance18;
    /* 033 */   private org.apache.spark.sql.execution.vectorized.ColumnVector scan_colInstance19;
    /* 034 */   private org.apache.spark.sql.execution.vectorized.ColumnVector scan_colInstance20;
    /* 035 */   private org.apache.spark.sql.execution.vectorized.ColumnVector scan_colInstance21;
    /* 036 */   private org.apache.spark.sql.execution.vectorized.ColumnVector scan_colInstance22;
    /* 037 */   private UnsafeRow scan_result;
    /* 038 */   //private org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder scan_holder;
    /* 039 */   //private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter scan_rowWriter;
    /* 040 */
/* 041 */   public GeneratedIteratorStoreSalesNoWrite(Object[] references) {
/* 042 */     this.references = references;
/* 043 */   }
    /* 044 */
/* 045 */   public void init(int index, scala.collection.Iterator[] inputs) {
/* 046 */     partitionIndex = index;
/* 047 */     this.inputs = inputs;
/* 048 */     scan_input = inputs[0];
/* 049 */     this.scan_numOutputRows = (org.apache.spark.sql.execution.metric.SQLMetric) references[0];
/* 050 */     this.scan_scanTime = (org.apache.spark.sql.execution.metric.SQLMetric) references[1];
/* 051 */     scan_scanTime1 = 0;
/* 052 */     scan_batch = null;
/* 053 */     scan_batchIdx = 0;
/* 054 */     scan_colInstance0 = null;
/* 055 */     scan_colInstance1 = null;
/* 056 */     scan_colInstance2 = null;
/* 057 */     scan_colInstance3 = null;
/* 058 */     scan_colInstance4 = null;
/* 059 */     scan_colInstance5 = null;
/* 060 */     scan_colInstance6 = null;
/* 061 */     scan_colInstance7 = null;
/* 062 */     scan_colInstance8 = null;
/* 063 */     scan_colInstance9 = null;
/* 064 */     scan_colInstance10 = null;
/* 065 */     scan_colInstance11 = null;
/* 066 */     scan_colInstance12 = null;
/* 067 */     scan_colInstance13 = null;
/* 068 */     scan_colInstance14 = null;
/* 069 */     scan_colInstance15 = null;
/* 070 */     scan_colInstance16 = null;
/* 071 */     scan_colInstance17 = null;
/* 072 */     scan_colInstance18 = null;
/* 073 */     scan_colInstance19 = null;
/* 074 */     scan_colInstance20 = null;
/* 075 */     scan_colInstance21 = null;
/* 076 */     scan_colInstance22 = null;
/* 077 */     scan_result = new UnsafeRow(23);
/* 078 */     //this.scan_holder = new org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder(scan_result, 0);
/* 079 */     //this.scan_rowWriter = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(scan_holder, 23);
/* 080 */
/* 081 */   }
    /* 082 */
/* 083 */   private void scan_nextBatch() throws java.io.IOException {
/* 084 */     long getBatchStart = System.nanoTime();
/* 085 */     if (scan_input.hasNext()) {
/* 086 */       scan_batch = (org.apache.spark.sql.execution.vectorized.ColumnarBatch)scan_input.next();
/* 087 */       scan_numOutputRows.add(scan_batch.numRows());
/* 088 */       scan_batchIdx = 0;
/* 089 */       scan_colInstance0 = scan_batch.column(0);
/* 090 */       scan_colInstance1 = scan_batch.column(1);
/* 091 */       scan_colInstance2 = scan_batch.column(2);
/* 092 */       scan_colInstance3 = scan_batch.column(3);
/* 093 */       scan_colInstance4 = scan_batch.column(4);
/* 094 */       scan_colInstance5 = scan_batch.column(5);
/* 095 */       scan_colInstance6 = scan_batch.column(6);
/* 096 */       scan_colInstance7 = scan_batch.column(7);
/* 097 */       scan_colInstance8 = scan_batch.column(8);
/* 098 */       scan_colInstance9 = scan_batch.column(9);
/* 099 */       scan_colInstance10 = scan_batch.column(10);
/* 100 */       scan_colInstance11 = scan_batch.column(11);
/* 101 */       scan_colInstance12 = scan_batch.column(12);
/* 102 */       scan_colInstance13 = scan_batch.column(13);
/* 103 */       scan_colInstance14 = scan_batch.column(14);
/* 104 */       scan_colInstance15 = scan_batch.column(15);
/* 105 */       scan_colInstance16 = scan_batch.column(16);
/* 106 */       scan_colInstance17 = scan_batch.column(17);
/* 107 */       scan_colInstance18 = scan_batch.column(18);
/* 108 */       scan_colInstance19 = scan_batch.column(19);
/* 109 */       scan_colInstance20 = scan_batch.column(20);
/* 110 */       scan_colInstance21 = scan_batch.column(21);
/* 111 */       scan_colInstance22 = scan_batch.column(22);
/* 112 */
/* 113 */     }
/* 114 */     scan_scanTime1 += System.nanoTime() - getBatchStart;
/* 115 */   }
    /* 116 */
/* 117 */   protected void processNext() throws java.io.IOException {
/* 118 */     if (scan_batch == null) {
/* 119 */       scan_nextBatch();
/* 120 */     }
/* 121 */     while (scan_batch != null) {
/* 122 */       int scan_numRows = scan_batch.numRows();
/* 123 */       int scan_localEnd = scan_numRows - scan_batchIdx;
/* 124 */       for (int scan_localIdx = 0; scan_localIdx < scan_localEnd; scan_localIdx++) {
/* 125 */         int scan_rowIdx = scan_batchIdx + scan_localIdx;
/* 126 */         boolean scan_isNull = scan_colInstance0.isNullAt(scan_rowIdx);
/* 127 */         int scan_value = scan_isNull ? -1 : (scan_colInstance0.getInt(scan_rowIdx));
/* 128 */         boolean scan_isNull1 = scan_colInstance1.isNullAt(scan_rowIdx);
/* 129 */         int scan_value1 = scan_isNull1 ? -1 : (scan_colInstance1.getInt(scan_rowIdx));
/* 130 */         boolean scan_isNull2 = scan_colInstance2.isNullAt(scan_rowIdx);
/* 131 */         int scan_value2 = scan_isNull2 ? -1 : (scan_colInstance2.getInt(scan_rowIdx));
/* 132 */         boolean scan_isNull3 = scan_colInstance3.isNullAt(scan_rowIdx);
/* 133 */         int scan_value3 = scan_isNull3 ? -1 : (scan_colInstance3.getInt(scan_rowIdx));
/* 134 */         boolean scan_isNull4 = scan_colInstance4.isNullAt(scan_rowIdx);
/* 135 */         int scan_value4 = scan_isNull4 ? -1 : (scan_colInstance4.getInt(scan_rowIdx));
/* 136 */         boolean scan_isNull5 = scan_colInstance5.isNullAt(scan_rowIdx);
/* 137 */         int scan_value5 = scan_isNull5 ? -1 : (scan_colInstance5.getInt(scan_rowIdx));
/* 138 */         boolean scan_isNull6 = scan_colInstance6.isNullAt(scan_rowIdx);
/* 139 */         int scan_value6 = scan_isNull6 ? -1 : (scan_colInstance6.getInt(scan_rowIdx));
/* 140 */         boolean scan_isNull7 = scan_colInstance7.isNullAt(scan_rowIdx);
/* 141 */         int scan_value7 = scan_isNull7 ? -1 : (scan_colInstance7.getInt(scan_rowIdx));
/* 142 */         boolean scan_isNull8 = scan_colInstance8.isNullAt(scan_rowIdx);
/* 143 */         int scan_value8 = scan_isNull8 ? -1 : (scan_colInstance8.getInt(scan_rowIdx));
/* 144 */         boolean scan_isNull9 = scan_colInstance9.isNullAt(scan_rowIdx);
/* 145 */         long scan_value9 = scan_isNull9 ? -1L : (scan_colInstance9.getLong(scan_rowIdx));
/* 146 */         boolean scan_isNull10 = scan_colInstance10.isNullAt(scan_rowIdx);
/* 147 */         int scan_value10 = scan_isNull10 ? -1 : (scan_colInstance10.getInt(scan_rowIdx));
/* 148 */         boolean scan_isNull11 = scan_colInstance11.isNullAt(scan_rowIdx);
/* 149 */         Decimal scan_value11 = scan_isNull11 ? null : (scan_colInstance11.getDecimal(scan_rowIdx, 7, 2));
/* 150 */         boolean scan_isNull12 = scan_colInstance12.isNullAt(scan_rowIdx);
/* 151 */         Decimal scan_value12 = scan_isNull12 ? null : (scan_colInstance12.getDecimal(scan_rowIdx, 7, 2));
/* 152 */         boolean scan_isNull13 = scan_colInstance13.isNullAt(scan_rowIdx);
/* 153 */         Decimal scan_value13 = scan_isNull13 ? null : (scan_colInstance13.getDecimal(scan_rowIdx, 7, 2));
/* 154 */         boolean scan_isNull14 = scan_colInstance14.isNullAt(scan_rowIdx);
/* 155 */         Decimal scan_value14 = scan_isNull14 ? null : (scan_colInstance14.getDecimal(scan_rowIdx, 7, 2));
/* 156 */         boolean scan_isNull15 = scan_colInstance15.isNullAt(scan_rowIdx);
/* 157 */         Decimal scan_value15 = scan_isNull15 ? null : (scan_colInstance15.getDecimal(scan_rowIdx, 7, 2));
/* 158 */         boolean scan_isNull16 = scan_colInstance16.isNullAt(scan_rowIdx);
/* 159 */         Decimal scan_value16 = scan_isNull16 ? null : (scan_colInstance16.getDecimal(scan_rowIdx, 7, 2));
/* 160 */         boolean scan_isNull17 = scan_colInstance17.isNullAt(scan_rowIdx);
/* 161 */         Decimal scan_value17 = scan_isNull17 ? null : (scan_colInstance17.getDecimal(scan_rowIdx, 7, 2));
/* 162 */         boolean scan_isNull18 = scan_colInstance18.isNullAt(scan_rowIdx);
/* 163 */         Decimal scan_value18 = scan_isNull18 ? null : (scan_colInstance18.getDecimal(scan_rowIdx, 7, 2));
/* 164 */         boolean scan_isNull19 = scan_colInstance19.isNullAt(scan_rowIdx);
/* 165 */         Decimal scan_value19 = scan_isNull19 ? null : (scan_colInstance19.getDecimal(scan_rowIdx, 7, 2));
/* 166 */         boolean scan_isNull20 = scan_colInstance20.isNullAt(scan_rowIdx);
/* 167 */         Decimal scan_value20 = scan_isNull20 ? null : (scan_colInstance20.getDecimal(scan_rowIdx, 7, 2));
/* 168 */         boolean scan_isNull21 = scan_colInstance21.isNullAt(scan_rowIdx);
/* 169 */         Decimal scan_value21 = scan_isNull21 ? null : (scan_colInstance21.getDecimal(scan_rowIdx, 7, 2));
/* 170 */         boolean scan_isNull22 = scan_colInstance22.isNullAt(scan_rowIdx);
/* 171 */         Decimal scan_value22 = scan_isNull22 ? null : (scan_colInstance22.getDecimal(scan_rowIdx, 7, 2));
                this._sum+=scan_value;
                this._sum+=scan_value1;
                this._sum+=scan_value2;
                this._sum+=scan_value3;
                this._sum+=scan_value4;
                this._sum+=scan_value5;
                this._sum+=scan_value6;
                this._sum+=scan_value7;
                this._sum+=scan_value8;
                this._sum+=scan_value9;
                this._sum+=scan_value10;
                if(!scan_isNull11){this._sum+=scan_value11.toUnscaledLong(); this._validDecimal+=1;}
                if(!scan_isNull12){this._sum+=scan_value12.toUnscaledLong(); this._validDecimal+=1;}
                if(!scan_isNull13){this._sum+=scan_value13.toUnscaledLong(); this._validDecimal+=1;}
                if(!scan_isNull14){this._sum+=scan_value14.toUnscaledLong(); this._validDecimal+=1;}
                if(!scan_isNull15){this._sum+=scan_value15.toUnscaledLong(); this._validDecimal+=1;}
                if(!scan_isNull16){this._sum+=scan_value16.toUnscaledLong(); this._validDecimal+=1;}
                if(!scan_isNull17){this._sum+=scan_value17.toUnscaledLong(); this._validDecimal+=1;}
                if(!scan_isNull18){this._sum+=scan_value18.toUnscaledLong(); this._validDecimal+=1;}
                if(!scan_isNull19){this._sum+=scan_value19.toUnscaledLong(); this._validDecimal+=1;}
                if(!scan_isNull20){this._sum+=scan_value20.toUnscaledLong(); this._validDecimal+=1;}
                if(!scan_isNull21){this._sum+=scan_value21.toUnscaledLong(); this._validDecimal+=1;}
                if(!scan_isNull22){this._sum+=scan_value22.toUnscaledLong(); this._validDecimal+=1;}

/* 311 */         append(scan_result);
/* 312 */         if (shouldStop()) { scan_batchIdx = scan_rowIdx + 1; return; }
/* 313 */       }
/* 314 */       scan_batchIdx = scan_numRows;
/* 315 */       scan_batch = null;
/* 316 */       scan_nextBatch();
/* 317 */     }
/* 318 */     scan_scanTime.add(scan_scanTime1 / (1000 * 1000));
/* 319 */     scan_scanTime1 = 0;
/* 320 */   }
public long getSum(){
    return this._sum;
}
    public long getValid(){
        return this._validDecimal;
    }

/* 321 */ }
