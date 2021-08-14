//** Matrix Storage  */
//** Store data in matrix way, with row, col coords. */
//** Rows could be with different sizes in sake of memory utilization */

#[derive(Debug)]
pub struct MatrixStorage<T> {
    matrix: Vec<Vec<Option<T>>>
}

impl<T: Copy + Clone> MatrixStorage<T> {
    pub fn new() -> Self {
        let matrix = Vec::new();
        MatrixStorage {
            matrix
        }
    }

    pub fn add(&mut self, col: usize, row: usize, value: T) {
        if row >= self.matrix.len() {
            self.matrix.resize(row + 1, Vec::new());
        }
        let row_vector = &mut self.matrix[row];
        if col >= row_vector.len() {
            row_vector.resize(col + 1, None);
        }
        self.matrix[row][col] = Some(value);
  }

    pub fn get(&self, col: usize, row: usize) -> Option<T> {
        if row < self.matrix.len() {
            let row_vector = &self.matrix[row];
            if col < row_vector.len() {
                return row_vector[col];
            } 
        }
        None
    }

    /// Return row vector 
    /// If 'row' exceeds length of array None returned
    pub fn row(&self, row: usize) -> Option<&Vec<Option<T>>> {
        if row < self.matrix.len() {
            Some(&self.matrix[row])
        } else {
            None
        }
    }

    /// Return amount of rows in the matrix
    pub fn rows_count(&self) -> usize {
        self.matrix.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_and_get_value() {
        let mut mstorage = MatrixStorage::new();

        mstorage.add(2, 3, true);

        let value = mstorage.get(2, 3);
        assert_eq!(value, Some(true));
    }

    #[test]
    fn test_return_none() {
        let mut mstorage = MatrixStorage::new();
        mstorage.add(2, 3, true);

        assert_eq!(mstorage.get(1, 2), None);
    }
}