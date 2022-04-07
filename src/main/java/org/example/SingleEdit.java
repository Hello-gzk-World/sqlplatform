package org.example;

import javafx.util.Pair;
import org.example.edit.*;
import org.example.node.select.PlainSelect;

import java.util.ArrayList;
import java.util.List;

/**
 * @author shenyichen
 * @date 2021/12/9
 **/
public class SingleEdit {

    public static List<Pair<PlainSelect,Float>> singleEdit(PlainSelect instr, PlainSelect student, Env env) throws Exception {
        List<Pair<PlainSelect,Float>> edits = new ArrayList<>();
        edits.addAll(editDistinct(instr, student));
        edits.addAll(editSelections(instr, student, env));
        if (instr.from != null) {
            edits.addAll(editFrom(instr, student, env));
        }
        edits.addAll(editConditions(instr, student, env));
        edits.addAll(editGroupBy(instr, student, env));
        edits.addAll(editOrderBy(instr, student, env));
        edits.addAll(editLimit(instr, student));
        for (Pair<PlainSelect,Float> edit: edits){
            Canonicalizer.canonicalize(edit.getKey(), env);
        }
        return edits;
    }

    public static List<String> hint(PlainSelect now, PlainSelect prev, Env env) throws Exception {
        List<String> edits = new ArrayList<>();
        try {
            edits.addAll(hintDistinct(now, prev));
            edits.addAll(hintSelections(now, prev, env));
            if (now.from != null) {
                edits.addAll(hintFrom(now, prev, env));
            }
            edits.addAll(hintConditions(now, prev, env));
            edits.addAll(hintGroupBy(now, prev, env));
            edits.addAll(hintOrderBy(now, prev, env));
            edits.addAll(hintLimit(now, prev));
        }
        catch (Exception e) {
            e.printStackTrace();
            return edits;
        }
        return edits;
    }

    public static List<Pair<PlainSelect,Float>> editDistinct(PlainSelect instr, PlainSelect student) {
        List<Pair<PlainSelect,Float>> edits = new ArrayList<>();
        if (instr.distinct != student.distinct) {
            PlainSelect edited = student.clone();
            edited.distinct = instr.distinct;
            edits.add(new Pair<>(edited,1.0f));
        }
        return edits;
    }

    public static List<String> hintDistinct(PlainSelect now, PlainSelect prev) {
        List<String> hints = new ArrayList<>();
        if (now.distinct != prev.distinct) {
            if (prev.distinct)
                hints.add("请尝试去掉distinct");
            hints.add("请尝试加上distinct");
        }
        return hints;
    }

    public static List<Pair<PlainSelect,Float>> editSelections(PlainSelect instr, PlainSelect student, Env env) {
        List<Pair<PlainSelect,Float>> edits = new ArrayList<>();
        SelectionEdit edit = new SelectionEdit();
        edits.addAll(edit.add(instr, student, env));
        edits.addAll(edit.remove(instr, student, env));
        edits.addAll(edit.edit(instr, student, env));
        return edits;
    }

    public static List<String> hintSelections(PlainSelect now, PlainSelect prev, Env env) {
        List<String> hints = new ArrayList<>();
        SelectionEdit edit = new SelectionEdit();
        hints.addAll(edit.hintAdd(now, prev, env));
        hints.addAll(edit.hintRemove(now, prev, env));
        hints.addAll(edit.hintEdit(now, prev, env));
        return hints;
    }

    public static List<Pair<PlainSelect,Float>> editFrom(PlainSelect instr, PlainSelect student, Env env) throws Exception {
        List<Pair<PlainSelect,Float>> edits = new ArrayList<>();

        TableEdit tableEdit = new TableEdit();
        edits.addAll(tableEdit.add(instr, student, env));
        edits.addAll(tableEdit.remove(instr, student, env));
        edits.addAll(tableEdit.edit(instr, student, env));

        JoinTypeEdit joinTypeEdit = new JoinTypeEdit();
        Pair<PlainSelect,Float> typeEdit = joinTypeEdit.typeEdit(instr, student, env);
        if (typeEdit != null)
            edits.add(typeEdit);

        return edits;
    }

    public static List<String> hintFrom(PlainSelect instr, PlainSelect student, Env env) throws Exception {
        List<String> hints = new ArrayList<>();

        TableEdit tableEdit = new TableEdit();
        hints.addAll(tableEdit.hintAdd(instr, student, env));
        hints.addAll(tableEdit.hintRemove(instr, student, env));
        hints.addAll(tableEdit.hintEdit(instr, student, env));

        JoinTypeEdit joinTypeEdit = new JoinTypeEdit();
        hints.addAll(joinTypeEdit.hintTypeEdit(instr, student, env));
        return hints;
    }

    public static List<Pair<PlainSelect,Float>> editConditions(PlainSelect instr, PlainSelect student, Env env) throws Exception {
        ConditionEdit conditionEdit = new ConditionEdit(instr, student, env);
        List<Pair<PlainSelect,Float>> res = conditionEdit.singleEdit();
        for (Pair<PlainSelect, Float> item: res) {
            PlainSelect select = item.getKey();
            if (select.where != null) {
                select.where = select.where.rearrange();
            }
        }
        return res;
    }

    public static List<String> hintConditions(PlainSelect instr, PlainSelect student, Env env) throws Exception {
        ConditionEdit conditionEdit = new ConditionEdit(instr, student, env);
        return conditionEdit.hint();
    }

    public static List<Pair<PlainSelect,Float>> editGroupBy(PlainSelect instr, PlainSelect student, Env env) {
        List<Pair<PlainSelect,Float>> edits = new ArrayList<>();
        GroupByEdit edit = new GroupByEdit();
        edits.addAll(edit.add(instr, student, env));
        edits.addAll(edit.remove(instr, student, env));
        edits.addAll(edit.edit(instr, student, env));
        return edits;
    }

    public static List<String> hintGroupBy(PlainSelect instr, PlainSelect student, Env env) throws Exception {
        List<String> hints = new ArrayList<>();
        GroupByEdit edit = new GroupByEdit();
        hints.addAll(edit.hintAdd(instr, student, env));
        hints.addAll(edit.hintRemove(instr, student, env));
        hints.addAll(edit.hintEdit(instr, student, env));
        return hints;
    }

    public static List<Pair<PlainSelect,Float>> editOrderBy(PlainSelect instr, PlainSelect student, Env env) {
        List<Pair<PlainSelect,Float>> edits = new ArrayList<>();
        OrderByEdit edit = new OrderByEdit();
        edits.addAll(edit.add(instr, student, env));
        edits.addAll(edit.remove(instr, student, env));
        edits.addAll(edit.edit(instr, student, env));
        return edits;
    }

    public static List<String> hintOrderBy(PlainSelect instr, PlainSelect student, Env env) throws Exception {
        List<String> hints = new ArrayList<>();
        OrderByEdit edit = new OrderByEdit();
        hints.addAll(edit.hintAdd(instr, student, env));
        hints.addAll(edit.hintRemove(instr, student, env));
        hints.addAll(edit.hintEdit(instr, student, env));
        return hints;
    }

    public static List<Pair<PlainSelect,Float>> editLimit(PlainSelect instr, PlainSelect student) {
        List<Pair<PlainSelect,Float>> edits = new ArrayList<>();
        PlainSelect edited = student.clone();
        float cost = 0.0f;
        if (instr.limit.rowCount == null) {
            if (student.limit.rowCount != null)
                cost += 0.5 *CostConfig.delete_cost_rate;
        } else {
            if (! instr.limit.rowCount.equals(student.limit.rowCount)){
                edited.limit.rowCount = instr.limit.rowCount;
                cost += 0.5;
            }
        }
        if (instr.limit.offset == null) {
            if (student.limit.offset != null)
                cost += 0.5 *CostConfig.delete_cost_rate;
        } else {
            if (! instr.limit.offset.equals(student.limit.offset)){
                edited.limit.offset = instr.limit.offset;
                cost += 0.5;
            }
        }
        if (cost > 0){
            edits.add(new Pair<>(edited,cost));
        }
        return edits;
    }

    public static List<String> hintLimit(PlainSelect instr, PlainSelect student) {
        List<String> hints = new ArrayList<>();
        if (instr.limit.rowCount == null) {
            if (student.limit.rowCount != null)
                hints.add("请尝试在limit中删去rowCount项");
        } else {
            if (! instr.limit.rowCount.equals(student.limit.rowCount)){
                hints.add("请尝试在limit中加入/修改rowCount项");
            }
        }
        if (instr.limit.offset == null) {
            if (student.limit.offset != null)
                hints.add("请尝试在limit中删去offset项");
        } else {
            if (! instr.limit.offset.equals(student.limit.offset)){
                hints.add("请尝试在limit中加入/修改offset项");
            }
        }
        return hints;
    }
}
