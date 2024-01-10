package projects.dao;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import projects.entity.Category;
import projects.entity.Material;
import projects.entity.Project;
import projects.entity.Step;
import projects.exception.DbException;
import provided.util.DaoBase;

public class ProjectDao extends DaoBase {

	private static final String CATEGORY_TABLE = "category";
	private static final String MATERIAL_TABLE = "material";
	private static final String PROJECT_TABLE = "project";
	private static final String PROJECT_CATEGORY_TABLE = "project_category";
	private static final String STEP_TABLE ="step";	
	
	public Project insertProject(Project project) {
		//@formatter:off
		String sql = ""
			+ "INSERT INTO " + PROJECT_TABLE + " "
			+ "(project_name, estimated_hours, actual_hours, difficulty, notes) "
			+ "VALUES "
			+ "(?, ?, ?, ?, ?)";
		//@formatter:on		
		
		
		//Here be errors
		try(Connection conn = DbConnection.getConnection()) {
			
			startTransaction(conn);
			
			try(PreparedStatement stmt = conn.prepareStatement(sql)) {
				
				setParameter(stmt, 1, project.getProjectName(), String.class);
				setParameter(stmt, 2, project.getEstimatedHours(), BigDecimal.class);
				setParameter(stmt, 3, project.getActualHours(), BigDecimal.class);
				setParameter(stmt, 4, project.getDifficulty(), Integer.class);
				setParameter(stmt, 5, project.getNotes(), String.class);
				
				stmt.executeUpdate();
				
				Integer projectId = getLastInsertId(conn, PROJECT_TABLE);
				commitTransaction(conn);
				
				project.setProjectId(projectId);
				return project;
				
			} catch (Exception e) {
				rollbackTransaction(conn);
				throw new DbException(e);
			}
		} catch(SQLException e) {
			throw new DbException(e);
		}
	}

	public List<Project> fetchAllProjects() {
		//@formatter:off
		String sql = ""
			+ ("SELECT * FROM " + PROJECT_TABLE);
		//@formatter:on
		
		
		try(Connection conn = DbConnection.getConnection()) {
			
			startTransaction(conn);
			
			try(PreparedStatement stmt = conn.prepareStatement(sql)) {
				
				try(ResultSet resultSet = stmt.executeQuery()) {
					
					List<Project> projects = new ArrayList<Project>();
					
					while (resultSet.next()) {
						projects.add(extract(resultSet, Project.class));
					}
					
					return projects;					
					
				} catch (SQLException e) {
					throw new DbException(e);
				}
				
			} catch (SQLException e) {
				rollbackTransaction(conn);
				throw new DbException(e);
			}
			
		} catch (SQLException e) {
			throw new DbException(e);
		}
		
		
	}

	public Optional<Project> fetchProjectById(Integer projectId) {
		String sql = "SELECT * FROM " + PROJECT_TABLE + " WHERE project_id = ?";
		
		try(Connection conn = DbConnection.getConnection()) {
			startTransaction(conn);
			
			try {
				Project project = null;
				
				try(PreparedStatement stmt = conn.prepareStatement(sql)) {
					setParameter(stmt, 1, projectId, Integer.class);
					
					try(ResultSet resultSet = stmt.executeQuery()) {
						if (resultSet.next()) {
							project = extract(resultSet, Project.class);
						}
					}
				}
				
				if(Objects.nonNull(project)) {
					project.getMaterials().addAll(fetchMaterialsForProject(conn, projectId));
					project.getSteps().addAll(fetchStepsForProject(conn, projectId));
					project.getCategories().addAll(fetchCategoriesForProject(conn, projectId));
				}
									
				commitTransaction(conn);
				return Optional.ofNullable(project);
				
			} catch(Exception e) {
				rollbackTransaction(conn);
				throw new DbException(e);
			}
		} catch(Exception e) {
			throw new DbException(e);
		}
	}

	private List<Category> fetchCategoriesForProject(Connection conn, Integer projectId)
		throws SQLException {
		//@formatter:off
		String sql = "SELECT * FROM " + CATEGORY_TABLE + " c " 
			+ "JOIN " + PROJECT_CATEGORY_TABLE + " pc USING (category_id) "
			+ "WHERE project_id = ?";
		//@formatter:on
		
		try(PreparedStatement stmt = conn.prepareStatement(sql)) {
			setParameter(stmt, 1, projectId, Integer.class);
			
			try(ResultSet resultSet = stmt.executeQuery()) {
				List<Category> categories = new ArrayList<Category>();
			
				while (resultSet.next()) {
					categories.add(extract(resultSet, Category.class));
				}
				
				return categories;			
			}
		}
	}

	private List<Step> fetchStepsForProject(Connection conn, Integer projectId)
		throws SQLException {
			//@formatter:off
			String sql = "SELECT * FROM " + STEP_TABLE + " WHERE project_id = ?";
			//@formatter:on
			
			try(PreparedStatement stmt = conn.prepareStatement(sql)) {
				setParameter(stmt, 1, projectId, Integer.class);
				
				try(ResultSet resultSet = stmt.executeQuery()) {
					List<Step> steps = new ArrayList<Step>();
				
					while (resultSet.next()) {
						steps.add(extract(resultSet, Step.class));
					}
					
					return steps;			
				}
			}
	}

	private List<Material> fetchMaterialsForProject(Connection conn, Integer projectId) 
			throws SQLException {
		//@formatter:off
		String sql = "SELECT * FROM " + MATERIAL_TABLE + " WHERE project_id = ?";
		//@formatter:on
		
		try(PreparedStatement stmt = conn.prepareStatement(sql)) {
			setParameter(stmt, 1, projectId, Integer.class);
			
			try(ResultSet resultSet = stmt.executeQuery()) {
				List<Material> materials = new ArrayList<Material>();
			
				while (resultSet.next()) {
					materials.add(extract(resultSet, Material.class));
				}
				
				return materials;			
			}
		}
	}

	public boolean modifyProjectDetails(Project project) {
		//@formatter:off
		String sql = ""
			+ "UPDATE " + PROJECT_TABLE + " SET "
			+ "project_name = ?, "
			+ "estimated_hours = ?, "
			+ "actual_hours = ?, "
			+ "difficulty = ?, "
			+ "notes = ? "
			+ "WHERE project_id = ?";
		//@formatter:on
		
		try(Connection conn = DbConnection.getConnection()) {
			
			startTransaction(conn);
			
			try(PreparedStatement stmt = conn.prepareStatement(sql)) {
				
				setParameter(stmt, 1, project.getProjectName(), String.class);
				setParameter(stmt, 2, project.getEstimatedHours(), BigDecimal.class);
				setParameter(stmt, 3, project.getActualHours(), BigDecimal.class);
				setParameter(stmt, 4, project.getDifficulty(), Integer.class);
				setParameter(stmt, 5, project.getNotes(), String.class);
				setParameter(stmt, 6, project.getProjectId(), Integer.class);
				
				boolean isSuccessful = stmt.executeUpdate() == 1;
				
				commitTransaction(conn);
				
				return isSuccessful;			
				
			} catch (SQLException e) {
				rollbackTransaction(conn);
				throw new DbException(e);
			}
			
		} catch(SQLException e) {
			throw new DbException(e);
		}
	}

	public boolean deleteProject(Integer projectId) {
		//@formatter:off
		String sql = ""
			+ "DELETE FROM " + PROJECT_TABLE + " WHERE project_id = ?";
		//@formatter:on
		
		try(Connection conn = DbConnection.getConnection()) {
			
			startTransaction(conn);
			
			try(PreparedStatement stmt = conn.prepareStatement(sql)) {
				
				setParameter(stmt, 1, projectId, Integer.class);
				
				boolean isSuccessful = stmt.executeUpdate() == 1;
				
				commitTransaction(conn);
				
				return isSuccessful;
				
			} catch(SQLException e) {
				rollbackTransaction(conn);
				
				throw new DbException(e);
			}
		} catch(SQLException e) {
			throw new DbException(e);
		}
	} 

}
