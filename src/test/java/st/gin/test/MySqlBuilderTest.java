package st.gin.test;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import st.gin.dto.SearchParam;
import st.gin.utils.MySqlBuilder;

import java.util.Map;

public class MySqlBuilderTest {

    @PersistenceContext
    protected EntityManager em;

    private final Map<String, String> findOrderByMap = Map.ofEntries(
            Map.entry("-default", "l.created_at"), // either default or -default must be kept. - indicates DESC
            Map.entry("stage", "l.current_stage"),
            Map.entry("loanApplicationDate", "l.loan_application_date"),
            Map.entry("loanAmount", "l.loan_amount"),
            Map.entry("status", "l.status"),
            Map.entry("productCode", "l.product_code"),
            Map.entry("lastStageChangedAt", "l.last_stage_changed_at")
    );

    public Page<LoanSearchSchema> find(SearchParam params, Pageable pageRequest) {
        MySqlBuilder sqlBuilder = MySqlBuilder.build(params)
                .count("wf.id")
                .select(
                        "wf.id",
                        "wf.process_type",
                        "wf.process_name",
                        "wf.current_stage",
                        "wf.previous_stage",
                        "wf.reference_key",
                        "wf.remarks",
                        "l.id as loan_id",
                        " l.status",
                        " l.account_number",
                        "l.valuator",
                        "l.is_closed",
                        "l.sanction_date",
                        "l.loan_application_date",
                        "l.loan_amount",
                        "l.product_code",
                        "l.partner_code",
                        "l.process_name as loan_process_name",
                        "l.process_type as loan_process_type",
                        "l.transaction_type",
                        "l.created_by",
                        "l.created_at",
                        "l.loan_type",
                        "l.lead_source",
                        "l.screening_date",
                        "l.urn_no",
                        "l.max_emi",
                        "l.base_loan_account",
                        "l.maximum_stage_order",
                        "l.priority",
                        "l.last_stage_changed_at",
                        "c.first_name",
                        "c.kgfs_name",
                        "c.locality",
                        "c.village_name",
                        "lcm.centre_code",
                        "l.customer_id",
                        "lcm.centre_name",
                        "c1.first_name as applicant_name",
                        "e.pincode",
                        "lcu.user_name as employee_name",
                        "l.old_account_no")
                .from("loan_accounts l")
                .innerJoin("wf_data AS wf", "l.id = wf.reference_key")
                .leftJoin("customer AS c", "l.customer_id = c.id")
                .leftJoin("enterprise e", "c.enterprise_id = e.id")
                .leftJoin("customer c1", "l.applicant = c1.urn_no")
                .leftJoin("customer_partner cp", "l.customer_id = cp.customer_id AND l.partner_code = cp.partner_code")
                .leftJoin("loan_centre lc", "l.id = lc.loan_id")
                .leftJoin("centre_master lcm", "lc.centre_id = lcm.id")
                .leftJoin("loan_account_addl lad", "lad.loan_id = l.id")
                .leftJoin("users lcu", "lcm.employee = lcu.user_id")
                .innerJoin("branchset_access bsa", "l.branch_id = bsa.child_branch_id")
                .innerJoin("user_branches ub", "bsa.branch_id = ub.branch_id")

                .where(" wf.current_stage", "stage")
                .where("l.is_closed", "isClosed")
                .where("l.loan_type", "loanType")
                .whereLike("c.first_name", "firstName", MySqlBuilder.LikePattern.End)
                .whereBetween("l.sanction_date", "sanctionDateFrom", "sanctionDateTo")
                .where("l.screening_date", "screeningDate")
                .where("l.urn_no", "urnNo")
                .where("l.created_by", "createdBy")
                .where("l.id", "loanId")
                .where("lad.partner_branch_name", "partnerBranchName")
                .where("lad.partner_branch_state", "partnerBranchState")
                .where("ub.user_id", "userId")

                .groupBy("l.id")
                .orderBy(findOrderByMap, params.opt("sortBy"));

        if (params.opt("branchId", Long::parseLong) != null) {
            sqlBuilder.innerJoin("branchset_access bs", "l.branch_id = bs.child_branch_id")
                    .where("bs.branch_id", "branchId");
        }

        return sqlBuilder.query(em, pageRequest, LoanSearchSchema.class);
    }

}
